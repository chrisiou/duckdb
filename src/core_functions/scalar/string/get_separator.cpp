#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "re2/re2.h"

namespace duckdb {

static string_t SeparatorGetter(const string_t &input, const bool as_regex) {
	auto option = input.GetString();

	// system's path separator
	auto fs = FileSystem::CreateLocal();
	auto system_sep = fs->PathSeparator(option);

	string separator;
	if (option == "system") {
		separator = system_sep;
	} else if (option == "forward_slash") {
		separator = "/";
	} else if (option == "backward_slash") {
		separator = "\\";
	} else { // (default) both_slash
		separator = "/\\";
	}

	if (as_regex) {
		separator = '[' + RE2::QuoteMeta(separator) + ']';
	}

	return separator;
}

static void GetSeparatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 0) {
		args.data.emplace_back(string_t("default"));
	}
	if (args.ColumnCount() == 1) {
		args.data.emplace_back(true);
	}
	auto &sep_vector = args.data[0];
	auto &regex_vector = args.data[1];
	BinaryExecutor::Execute<string_t, bool, string_t>(
	    sep_vector, regex_vector, result, args.size(),
	    [&](string_t sep, bool as_regex) { return StringVector::AddString(result, SeparatorGetter(sep, as_regex)); });
}

ScalarFunctionSet GetSeparatorFun::GetFunctions() {
	ScalarFunctionSet get_sep;
	get_sep.AddFunction(ScalarFunction({}, LogicalType::VARCHAR, GetSeparatorFunction));
	get_sep.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, GetSeparatorFunction));
	get_sep.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR, GetSeparatorFunction));
	return get_sep;
}

} // namespace duckdb
