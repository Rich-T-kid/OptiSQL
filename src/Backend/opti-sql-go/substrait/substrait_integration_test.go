package substrait

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ! find . -type f -name '*\.csv*' -delete

// IntegrationTest defines a single integration test case using buildTree
type IntegrationTest struct {
	name        string
	shouldError bool
	logicalPlan jsonOBJ
	sqlEquiv    string // SQL equivalent for documentation
}

// FileIntegrationTest defines a test case that reads from a Substrait JSON file
type FileIntegrationTest struct {
	name        string
	shouldError bool
	filePath    string
	sqlEquiv    string // Plan ID = SQL equivalent
}

// TestOperatorsIntegration tests each operator using buildTree with struct-based test cases
func TestOperatorsIntegration(t *testing.T) {
	defer func() {
		curDir, err := os.Getwd()
		if err != nil {
			fmt.Printf("Failed to get current directory: %v\n", err)
		}

		// Read directory contents
		entries, err := os.ReadDir(curDir)
		if err != nil {
			fmt.Printf("Failed to read directory: %v\n", err)
		}

		// Delete all files containing .csv in their name
		for _, entry := range entries {
			if !entry.IsDir() && strings.Contains(entry.Name(), "country_full.csv") {
				filePath := fmt.Sprintf("%s/%s", curDir, entry.Name())
				err := os.Remove(filePath)
				if err != nil {
					fmt.Printf("error removing %s: %v\n", entry.Name(), err)
				} else {
					fmt.Printf("deleted: %s\n", entry.Name())
				}
			}
		}
	}()
	t.Run("Filter Operator Integration", func(t *testing.T) {

		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		filterTests := []IntegrationTest{
			{
				name:        "SELECT * FROM country WHERE region > 'Africa'",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country WHERE region > 'Africa'",
				logicalPlan: map[string]any{
					"Operator": "Filter",
					"Filter": map[string]any{
						"input": sourceInput,
						"expression": map[string]any{
							"expr_type": "BinaryExpr",
							"op":        "GreaterThan",
							"left": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
							"right": map[string]any{
								"expr_type": "LiteralResolve",
								"value":     "Africa",
								"lit_type":  "string",
							},
						},
					},
				},
			},
			{
				name:        "SELECT * FROM country WHERE country_code < 500",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country WHERE country_code < 500",
				logicalPlan: map[string]any{
					"Operator": "Filter",
					"Filter": map[string]any{
						"input": sourceInput,
						"expression": map[string]any{
							"expr_type": "BinaryExpr",
							"op":        "LessThan",
							"left": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "country-code",
							},
							"right": map[string]any{
								"expr_type": "LiteralResolve",
								"value":     500,
								"lit_type":  "int",
							},
						},
					},
				},
			},
			{
				name:        "SELECT * FROM country WHERE name = 'Canada' AND region = 'Americas'",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country WHERE name = 'Canada' AND region = 'Americas'",
				logicalPlan: map[string]any{
					"Operator": "Filter",
					"Filter": map[string]any{
						"input": sourceInput,
						"expression": map[string]any{
							"expr_type": "BinaryExpr",
							"op":        "And",
							"left": map[string]any{
								"expr_type": "BinaryExpr",
								"op":        "Equal",
								"left": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
								"right": map[string]any{
									"expr_type": "LiteralResolve",
									"value":     "Canada",
									"lit_type":  "string",
								},
							},
							"right": map[string]any{
								"expr_type": "BinaryExpr",
								"op":        "Equal",
								"left": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "region",
								},
								"right": map[string]any{
									"expr_type": "LiteralResolve",
									"value":     "Americas",
									"lit_type":  "string",
								},
							},
						},
					},
				},
			},
			{
				name:        "Filter with missing expression field - should fail",
				shouldError: true,
				sqlEquiv:    "Filter with missing expression field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Filter",
					"Filter": map[string]any{
						"input": sourceInput,
					},
				},
			},
			{
				name:        "Filter with missing input field - should fail",
				shouldError: true,
				sqlEquiv:    "Filter with missing input field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Filter",
					"Filter": map[string]any{
						"expression": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "region",
						},
					},
				},
			},
		}

		for _, test := range filterTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if (err != nil) != test.shouldError {
					t.Errorf("buildTree error = %v, shouldError = %v", err, test.shouldError)
					return
				}

				if !test.shouldError && emitter != nil {
					rb, err := emitter.emitOperator.Next(5)
					if err != nil {
						t.Errorf("Next() error = %v", err)
						return
					}
					if rb != nil {
						t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
					}
				}
			})
		}
	})

	t.Run("Project Operator Integration", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		projectTests := []IntegrationTest{
			{
				name:        "SELECT name, region FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT name, region FROM country",
				logicalPlan: map[string]any{
					"Operator": "Project",
					"Project": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
					},
				},
			},
			{
				name:        "SELECT country_code, name, region FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT country_code, name, region FROM country",
				logicalPlan: map[string]any{
					"Operator": "Project",
					"Project": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "country-code",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
					},
				},
			},
			{
				name:        "SELECT * - all columns using single column projection",
				shouldError: false,
				sqlEquiv:    "SELECT * - all columns using single column projection",
				logicalPlan: map[string]any{
					"Operator": "Project",
					"Project": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
						},
					},
				},
			},
			{
				name:        "Project with missing expressions field - should fail",
				shouldError: true,
				sqlEquiv:    "Project with missing expressions field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Project",
					"Project": map[string]any{
						"input": sourceInput,
					},
				},
			},
			{
				name:        "Project with empty expressions array - should fail",
				shouldError: true,
				sqlEquiv:    "Project with empty expressions array - should fail",
				logicalPlan: map[string]any{
					"Operator": "Project",
					"Project": map[string]any{
						"input":       sourceInput,
						"expressions": []map[string]any{},
					},
				},
			},
		}

		for _, test := range projectTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if (err != nil) != test.shouldError {
					t.Errorf("buildTree error = %v, shouldError = %v", err, test.shouldError)
					return
				}

				if !test.shouldError && emitter != nil {
					rb, err := emitter.emitOperator.Next(5)
					if err != nil {
						t.Errorf("Next() error = %v", err)
						return
					}
					if rb != nil {
						t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
					}
				}
			})
		}
	})

	t.Run("Sort Operator Integration", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		sortTests := []IntegrationTest{
			{
				name:        "SELECT * FROM country ORDER BY name ASC",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country ORDER BY name ASC",
				logicalPlan: map[string]any{
					"Operator": "Sort",
					"Sort": map[string]any{
						"input": sourceInput,
						"by": []map[string]any{
							{
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
								"asc": true,
							},
						},
					},
				},
			},
			{
				name:        "SELECT * FROM country ORDER BY country_code DESC",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country ORDER BY country_code DESC",
				logicalPlan: map[string]any{
					"Operator": "Sort",
					"Sort": map[string]any{
						"input": sourceInput,
						"by": []map[string]any{
							{
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
								"asc": false,
							},
						},
					},
				},
			},
			{
				name:        "SELECT * FROM country ORDER BY region ASC, name DESC",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country ORDER BY region ASC, name DESC",
				logicalPlan: map[string]any{
					"Operator": "Sort",
					"Sort": map[string]any{
						"input": sourceInput,
						"by": []map[string]any{
							{
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "region",
								},
								"asc": true,
							},
							{
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
								"asc": false,
							},
						},
					},
				},
			},
			{
				name:        "Sort with missing by field - should fail",
				shouldError: true,
				sqlEquiv:    "Sort with missing by field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Sort",
					"Sort": map[string]any{
						"input": sourceInput,
					},
				},
			},
			{
				name:        "Sort with empty by array - should fail",
				shouldError: true,
				sqlEquiv:    "Sort with empty by array - should fail",
				logicalPlan: map[string]any{
					"Operator": "Sort",
					"Sort": map[string]any{
						"input": sourceInput,
						"by":    []map[string]any{},
					},
				},
			},
		}

		for _, test := range sortTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if (err != nil) != test.shouldError {
					t.Errorf("buildTree error = %v, shouldError = %v", err, test.shouldError)
					return
				}

				if !test.shouldError && emitter != nil {
					rb, err := emitter.emitOperator.Next(5)
					if err != nil {
						t.Errorf("Next() error = %v", err)
						return
					}
					if rb != nil {
						t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
					}
				}
			})
		}
	})

	t.Run("Distinct Operator Integration", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		distinctTests := []IntegrationTest{
			{
				name:        "SELECT DISTINCT region FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT DISTINCT region FROM country",
				logicalPlan: map[string]any{
					"Operator": "Distinct",
					"Distinct": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
					},
				},
			},
			{
				name:        "SELECT DISTINCT region, sub_region FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT DISTINCT region, sub_region FROM country",
				logicalPlan: map[string]any{
					"Operator": "Distinct",
					"Distinct": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "sub-region",
							},
						},
					},
				},
			},
			{
				name:        "SELECT DISTINCT * FROM country - all columns",
				shouldError: false,
				sqlEquiv:    "SELECT DISTINCT * FROM country - all columns",
				logicalPlan: map[string]any{
					"Operator": "Distinct",
					"Distinct": map[string]any{
						"input": sourceInput,
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "country-code",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "sub-region",
							},
						},
					},
				},
			},
			{
				name:        "Distinct with missing input field - should fail",
				shouldError: true,
				sqlEquiv:    "Distinct with missing input field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Distinct",
					"Distinct": map[string]any{
						"expressions": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
					},
				},
			},
		}

		for _, test := range distinctTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if (err != nil) != test.shouldError {
					t.Errorf("buildTree error = %v, shouldError = %v", err, test.shouldError)
					return
				}

				if !test.shouldError && emitter != nil {
					rb, err := emitter.emitOperator.Next(5)
					if err != nil {
						t.Errorf("Next() error = %v", err)
						return
					}
					if rb != nil {
						t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
					}
				}
			})
		}
	})

	t.Run("Limit Operator Integration", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		limitTests := []IntegrationTest{
			{
				name:        "SELECT * FROM country LIMIT 10",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country LIMIT 10",
				logicalPlan: map[string]any{
					"Operator": "Limit",
					"Limit": map[string]any{
						"input": sourceInput,
						"limit": 10,
					},
				},
			},
			{
				name:        "SELECT * FROM country LIMIT 1000000",
				shouldError: true,
				sqlEquiv:    "SELECT * FROM country LIMIT 1000000",
				logicalPlan: map[string]any{
					"Operator": "Limit",
					"Limit": map[string]any{
						"input": sourceInput,
						"limit": 1000000,
					},
				},
			},
			{
				name:        "SELECT * FROM country LIMIT 1 - edge case minimum",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM country LIMIT 1 - edge case minimum",
				logicalPlan: map[string]any{
					"Operator": "Limit",
					"Limit": map[string]any{
						"input": sourceInput,
						"limit": 1,
					},
				},
			},
			{
				name:        "Limit with missing limit field - should fail",
				shouldError: true,
				sqlEquiv:    "Limit with missing limit field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Limit",
					"Limit": map[string]any{
						"input": sourceInput,
					},
				},
			},
			{
				name:        "Limit with zero value - should fail",
				shouldError: true,
				sqlEquiv:    "Limit with zero value - should fail",
				logicalPlan: map[string]any{
					"Operator": "Limit",
					"Limit": map[string]any{
						"input": sourceInput,
						"limit": 0,
					},
				},
			},
		}

		for _, test := range limitTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if (err != nil) != test.shouldError {
					t.Errorf("buildTree error = %v, shouldError = %v", err, test.shouldError)
					return
				}

				if !test.shouldError && emitter != nil {
					rb, err := emitter.emitOperator.Next(5)
					if err != nil {
						t.Errorf("Next() error = %v", err)
						return
					}
					if rb != nil {
						t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
					}
				}
			})
		}
	})

	t.Run("GroupBy Operator Integration", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		groupByTests := []IntegrationTest{
			{
				name:        "SELECT region, COUNT(*) FROM country GROUP BY region",
				shouldError: false,
				sqlEquiv:    "SELECT region, COUNT(*) FROM country GROUP BY region",
				logicalPlan: map[string]any{
					"Operator": "GroupBy",
					"GroupBy": map[string]any{
						"input": sourceInput,
						"group_by": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
						"aggrs": []map[string]any{
							{
								"function": "Count",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
							},
						},
					},
				},
			},
			{
				name:        "SELECT region, sub_region, SUM(country_code) FROM country GROUP BY region, sub_region",
				shouldError: false,
				sqlEquiv:    "SELECT region, sub_region, SUM(country_code) FROM country GROUP BY region, sub_region",
				logicalPlan: map[string]any{
					"Operator": "GroupBy",
					"GroupBy": map[string]any{
						"input": sourceInput,
						"group_by": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
							{
								"expr_type": "ColumnResolve",
								"name":      "sub-region",
							},
						},
						"aggrs": []map[string]any{
							{
								"function": "Sum",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
						},
					},
				},
			},
			{
				name:        "SELECT region, COUNT(*), AVG(country_code), MIN(country_code) FROM country GROUP BY region",
				shouldError: false,
				sqlEquiv:    "SELECT region, COUNT(*), AVG(country_code), MIN(country_code) FROM country GROUP BY region",
				logicalPlan: map[string]any{
					"Operator": "GroupBy",
					"GroupBy": map[string]any{
						"input": sourceInput,
						"group_by": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
						"aggrs": []map[string]any{
							{
								"function": "Count",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
							},
							{
								"function": "Avg",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
							{
								"function": "Min",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
						},
					},
				},
			},
			{
				name:        "GroupBy with missing group_by field - should fail",
				shouldError: true,
				sqlEquiv:    "GroupBy with missing group_by field - should fail",
				logicalPlan: map[string]any{
					"Operator": "GroupBy",
					"GroupBy": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{
							{
								"function": "Count",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
							},
						},
					},
				},
			},
			{
				name:        "GroupBy with empty aggrs array - should fail",
				shouldError: true,
				sqlEquiv:    "GroupBy with empty aggrs array - should fail",
				logicalPlan: map[string]any{
					"Operator": "GroupBy",
					"GroupBy": map[string]any{
						"input": sourceInput,
						"group_by": []map[string]any{
							{
								"expr_type": "ColumnResolve",
								"name":      "region",
							},
						},
						"aggrs": []map[string]any{},
					},
				},
			},
		}

		for _, test := range groupByTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if test.shouldError {
					if err == nil {
						t.Errorf("%s should have errored but recieved nil", test.name)
					}
					return
				}
				if err != nil {
					t.Errorf("%s failed with error %v", test.name, err)
					return
				}

				rb, err := emitter.emitOperator.Next(5)
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				if rb != nil {
					t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
				}
			})
		}
	})

	t.Run("Aggregate Operator Integration (Global)", func(t *testing.T) {
		sourceInput := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "country_full.csv",
				"local":     false,
			},
		}

		aggregateTests := []IntegrationTest{
			{
				name:        "SELECT SUM(country_code) FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT SUM(country_code) FROM country",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{
							{
								"function": "Sum",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
						},
					},
				},
			},
			{
				name:        "SELECT COUNT(name) FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT COUNT(name) FROM country",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{
							{
								"function": "Count",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "name",
								},
							},
						},
					},
				},
			},
			{
				name:        "SELECT AVG(country_code) FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT AVG(country_code) FROM country",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{
							{
								"function": "Avg",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
						},
					},
				},
			},
			{
				name:        "SELECT MIN(country_code), MAX(country_code) FROM country",
				shouldError: false,
				sqlEquiv:    "SELECT MIN(country_code), MAX(country_code) FROM country",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{
							{
								"function": "Min",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
							{
								"function": "Max",
								"expr": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "country-code",
								},
							},
						},
					},
				},
			},
			{
				name:        "Aggregate with missing aggrs field - should fail",
				shouldError: true,
				sqlEquiv:    "Aggregate with missing aggrs field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
					},
				},
			},
			{
				name:        "Aggregate with empty aggrs array - should fail",
				shouldError: true,
				sqlEquiv:    "Aggregate with empty aggrs array - should fail",
				logicalPlan: map[string]any{
					"Operator": "Aggregate",
					"Aggregate": map[string]any{
						"input": sourceInput,
						"aggrs": []map[string]any{},
					},
				},
			},
		}

		for _, test := range aggregateTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if test.shouldError {
					if err == nil {
						t.Errorf("%s should have errored but received nil", test.name)
					}
					return
				}
				if err != nil {
					t.Errorf("%s failed with error %v", test.name, err)
					return
				}

				rb, err := emitter.emitOperator.Next(5)
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				if rb != nil {
					t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
				}
			})
		}
	})

	t.Run("Join Operator Integration", func(t *testing.T) {
		leftSource := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "user_test_data.csv",
				"local":     false,
			},
		}

		rightSource := map[string]any{
			"Operator": "Source",
			"Source": map[string]any{
				"file-name": "company_test_data.csv",
				"local":     false,
			},
		}

		joinTests := []IntegrationTest{
			{
				name:        "SELECT * FROM users JOIN companies ON users.id = companies.id",
				shouldError: false,
				sqlEquiv:    "SELECT * FROM users JOIN companies ON users.id = companies.id",
				logicalPlan: map[string]any{
					"Operator": "Join",
					"Join": map[string]any{
						"left":      leftSource,
						"right":     rightSource,
						"join_type": "Inner",
						"on": []map[string]any{
							{
								"left": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "id",
								},
								"right": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "id",
								},
							},
						},
					},
				},
			}, /*
					{
						name:        "Join with Filter on users.age_years > 25",
						shouldError: false,
						sqlEquiv:    "SELECT * FROM users JOIN companies ON users.id = companies.id WHERE age_years > 25",
						logicalPlan: map[string]any{
							"Operator": "Filter",
							"Filter": map[string]any{
								"input": map[string]any{
									"Operator": "Join",
									"Join": map[string]any{
										"left":      leftSource,
										"right":     rightSource,
										"join_type": "Inner",
										"on": []map[string]any{
											{
												"left": map[string]any{
													"expr_type": "ColumnResolve",
													"name":      "id",
												},
												"right": map[string]any{
													"expr_type": "ColumnResolve",
													"name":      "id",
												},
											},
										},
									},
								},
								"expression": map[string]any{
									"expr_type": "BinaryExpr",
									"op":        "GreaterThan",
									"left": map[string]any{
										"expr_type": "ColumnResolve",
										"name":      "age_years",
									},
									"right": map[string]any{
										"expr_type": "LiteralResolve",
										"value":     25,
										"lit_type":  "int",
									},
								},
							},
						},
					},
				{
					name:        "Join with Sort on username",
					shouldError: false,
					sqlEquiv:    "SELECT * FROM users JOIN companies ON users.id = companies.id ORDER BY username",
					logicalPlan: map[string]any{
						"Operator": "Sort",
						"Sort": map[string]any{
							"input": map[string]any{
								"Operator": "Join",
								"Join": map[string]any{
									"left":      leftSource,
									"right":     rightSource,
									"join_type": "Inner",
									"on": []map[string]any{
										{
											"left": map[string]any{
												"expr_type": "ColumnResolve",
												"name":      "id",
											},
											"right": map[string]any{
												"expr_type": "ColumnResolve",
												"name":      "id",
											},
										},
									},
								},
							},
							"by": []map[string]any{
								{
									"expr": map[string]any{
										"expr_type": "ColumnResolve",
										"name":      "username",
									},
									"asc": true,
								},
							},
						},
					},
				},*/
			{
				name:        "Join with missing left field - should fail",
				shouldError: true,
				sqlEquiv:    "Join with missing left field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Join",
					"Join": map[string]any{
						"right":     rightSource,
						"join_type": "Inner",
						"on": []map[string]any{
							{
								"left": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "id",
								},
								"right": map[string]any{
									"expr_type": "ColumnResolve",
									"name":      "id",
								},
							},
						},
					},
				},
			},
			{
				name:        "Join with missing on field - should fail",
				shouldError: true,
				sqlEquiv:    "Join with missing on field - should fail",
				logicalPlan: map[string]any{
					"Operator": "Join",
					"Join": map[string]any{
						"left":      leftSource,
						"right":     rightSource,
						"join_type": "Inner",
					},
				},
			},
		}

		for _, test := range joinTests {
			t.Run(test.sqlEquiv, func(t *testing.T) {
				planMetaData := NewPlanMetaData(test.sqlEquiv)
				emitter, err := buildTree(test.logicalPlan, planMetaData)

				if test.shouldError {
					if err == nil {
						t.Errorf("%s should have errored but received nil", test.name)
					}
					return
				}
				if err != nil {
					t.Errorf("%s failed with error %v", test.name, err)
					return
				}

				rb, err := emitter.emitOperator.Next(5)
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				if rb != nil {
					t.Logf("[%s]\n%s\n", test.sqlEquiv, rb.PrettyPrint())
				}
			})
		}
	})
}
func TestSubstraitFilesBasic(t *testing.T) {
	basePath := filepath.Join("..", "..", "test_data", "substrait_plans", "basic")

	basicFileTests := []FileIntegrationTest{
		{
			name:        "basic__00_test.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_00_test.json"),
			sqlEquiv:    "tbd",
		},

		{
			name:        "basic_01_filter_project_sort.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_01_source_filter.json"),
			sqlEquiv:    "tbd",
		},
		{
			name:        "basic_02_project.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_02_project.json"),
			sqlEquiv:    "",
		},
		{
			name:        "basic_03_sort.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_03_sort.json"),
			sqlEquiv:    "tbd",
		},
		{
			name:        "basic_04_distinct.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_04_distinct.json"),
			sqlEquiv:    "tbd",
		},
		{
			name:        "basic_05_limit.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_05_limit.json"),
			sqlEquiv:    "tbd",
		},
		{
			name:        "basic_06_aggr.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "basic_06_aggr.json"),
			sqlEquiv:    "tbd",
		},
	}
	for _, test := range basicFileTests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.Open(test.filePath)
			if err != nil {
				t.Logf("Skipping %s: file not found err :%v \n", test.name, err)
				return
			}
			defer file.Close()

			emitter, err := ConsumeSubstraitPlan(file)

			if (err != nil) != test.shouldError {
				t.Errorf("ConsumeSubstraitPlan error = %v, shouldError = %v", err, test.shouldError)
				return
			}

			if !test.shouldError && emitter != nil {
				rb, err := emitter.emitOperator.Next(5)
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				if rb != nil {
					t.Logf("[%s - %s]\n%s\n", test.sqlEquiv, test.name, rb.PrettyPrint())
				}
			}
		})
	}
}

// TestSubstraitFilesMedium tests reading and executing medium-complexity Substrait plans from JSON files
func TestSubstraitFilesMedium(t *testing.T) {
	basePath := filepath.Join("..", "..", "test_data", "substrait_plans", "medium")

	mediumFileTests := []FileIntegrationTest{
		/*{
			name:        "mid_01_filter_project_sort.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "mid_01_filter_project_sort.json"),
			sqlEquiv:    "tbd",
		},
		{
			name:        "mid_02_group_by_aggregate.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "mid_02_group_by_aggregate.json"),
			sqlEquiv:    "tbd",
		},
		*/{
			name:        "mid_03_join_filter.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "mid_03_join_filter.json"),
			sqlEquiv:    "tbd",
		},
		/*{
			name:        "mid_04_join_sort_limit.json",
			shouldError: false,
			filePath:    filepath.Join(basePath, "mid_04_join_sort_limit.json"),
			sqlEquiv:    "tbd",
		},*/
	}

	for _, test := range mediumFileTests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.Open(test.filePath)
			if err != nil {
				t.Logf("Skipping %s: file not found err :%v \n", test.name, err)
				return
			}
			defer file.Close()

			emitter, err := ConsumeSubstraitPlan(file)

			if (err != nil) != test.shouldError {
				t.Errorf("ConsumeSubstraitPlan error = %v, shouldError = %v", err, test.shouldError)
				return
			}

			if !test.shouldError && emitter != nil {
				rb, err := emitter.emitOperator.Next(5)
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				if rb != nil {
					t.Logf("[%s - %s]\n%s\n", test.sqlEquiv, test.name, rb.PrettyPrint())
				}
			}
		})
	}
}

// ConsumeSubstraitPlan reads a Substrait plan from an io.Reader and returns an Emitter
func ConsumeSubstraitPlan(reader io.Reader) (*Emiter, error) {
	// Read the JSON from the reader
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read plan: %w", err)
	}

	// Unmarshal into a map
	var planMap jsonOBJ
	err = json.Unmarshal(data, &planMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	// The plan should have an "Emit" key containing the operator tree
	if _, ok := planMap["Emit"]; !ok {
		return nil, fmt.Errorf("plan missing 'Emit' key")
	}

	emitObj := planMap["Emit"].(map[string]any)

	// Create plan metadata - use the first (and typically only) operator as ID
	var planID string
	for key := range emitObj {
		planID = key
		break
	}

	planMetaData := NewPlanMetaData(planID)

	// Build the tree starting from the Emit operator
	emitter, err := buildTree(emitObj, planMetaData)
	if err != nil {
		return nil, fmt.Errorf("failed to build tree: %w", err)
	}

	return emitter, nil
}
