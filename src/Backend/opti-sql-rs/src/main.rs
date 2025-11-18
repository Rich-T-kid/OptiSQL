//use datafusion::arrow::record_batch::RecordBatch;
use datafusion::{arrow::{array::RecordBatch, util::pretty::print_batches}, functions::crypto::basic, prelude::*};
use datafusion_substrait::*;
mod project;
#[allow(dead_code)]
#[tokio::main]
async fn main() {
    let mut ctx = SessionContext::new();

    ctx.register_csv(
        "example",
        "example.csv",
        CsvReadOptions::new()
    ).await.unwrap();
    // basic projections
    //basic_project("Basic column projection",&mut ctx,"select name,salary from example").await;
    //basic_project("reorder and duplicate projection",&mut ctx,"select salary,name,salary as s1 from example").await; // this is an error, expression names must be unique/ must use alias to get around error
    // Literal projections
    //basic_project("select int",&mut ctx,"select 1").await;
    //basic_project("select string",&mut ctx,"select 'hello'").await;
    //basic_project("reorder float",&mut ctx,"select 3.14").await;
    // Literal + column
    //basic_project("column plus literal",&mut ctx,"select salary + 10 from example").await;
    //basic_project("column times literal",&mut ctx,"select salary * 2.4 from example").await;
    // fully literal
    //basic_project("two literals",&mut ctx,"select 5 + 10").await;
    // column by column
    // basic_project("column plus column",&mut ctx,"select salary + age from example").await;
    //basic_project("Column-literal + nested arithmetic",&mut ctx,"SELECT (salary - age) * 1.08 FROM example;").await;
    // alias operators
    //basic_project("alias_1",&mut ctx,"SELECT age + 5 AS new_age FROM example").await;
    //basic_project("alias_2 constant",&mut ctx,"SELECT 1 as greatest_number").await;
    //String Expressions
    //basic_project("select upper()",&mut ctx,"SELECT upper('richard')").await;
    //basic_project("select lower()",&mut ctx,"SELECT lower(name)").await;
    //basic_project("select substring()",&mut ctx,"SELECT substring(name,1,3)").await;
    // mixed expressions
    //basic_project("mixed expressions",&mut ctx,"SELECT upper(name) AS upper_name, salary * 1.1 AS increased_salary FROM example").await;
    // function calls
    let (l,r) = basic_project("function call with Abs()",&mut ctx,"SELECT ABS(age) FROM example").await;
    //println!("Logical Plan:\n{}", l);
    print_batches(&r).unwrap();ß
    //basic_project("function call Round()",&mut ctx,"SELECT Round(age) FROM example").await;
    //basic_project("function call Length()",&mut ctx,"SELECT LENGTH(name) FROM example").await;

}

pub async fn basic_project(name : &str,ctx : &mut SessionContext,sql : &str) -> (String,Vec<RecordBatch>) {
    println!("Running project: {}",name);
    let df1 = ctx.sql(sql)
        .await
        .unwrap();


    let logical_plan = df1.logical_plan().clone();
    let substrait_plan = logical_plan::producer::to_substrait_plan(&logical_plan, &ctx.state()).unwrap();
    print!("Substrait Plan :\n{:?}", substrait_plan);


    let display = format!("{}",logical_plan.display_indent());
    
    // Running will create the physical plan automatically
    return (display,df1.collect().await.unwrap());
    
}
