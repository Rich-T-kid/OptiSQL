#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "SQLParser.hpp"

namespace py = pybind11;
using namespace optisql;

PYBIND11_MODULE(optisql_parser, m) {
    m.doc() = "OptiSQL Parser - SQL parser with optimization";

    py::class_<SQLParser>(m, "SQLParser")
        .def(py::init<>())
        .def("parse_to_json", &SQLParser::parseToJson,
             py::arg("sql"),
             py::arg("optimize") = true,
             "Parse SQL and return JSON string. Returns error JSON on parse failure.");

    m.def("parse", [](const std::string& sql, bool optimize) {
        SQLParser parser;
        return parser.parseToJson(sql, optimize);
    }, py::arg("sql"), py::arg("optimize") = true,
       "Parse SQL and return JSON string (convenience function)");

    m.def("parse_dict", [](const std::string& sql, bool optimize) -> py::object {
        SQLParser parser;
        auto result = parser.parse(sql, optimize);

        if (!result) {
            py::dict error;
            error["error"] = true;
            error["message"] = result.error().message;
            error["line"] = result.error().location.line;
            error["column"] = result.error().location.column;
            return error;
        }

        return py::module_::import("json").attr("loads")(result->ast.dump());
    }, py::arg("sql"), py::arg("optimize") = true,
       "Parse SQL and return Python dict");
}
