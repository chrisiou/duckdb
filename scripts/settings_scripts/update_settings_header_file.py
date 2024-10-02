from collections import OrderedDict
from .config import SEPARATOR, SettingsList, find_start_end_indexes, write_content_to_file

# this OrderedDict maintains the original order of settings structure definitions from the 
# settings.hpp file, with any new names provided. It facilitates easier PR reviews.
# TODO: in a future PR, consider switching to an alphabetically ordered structure by 
# removing the reorder_settings() call in generate_content.
ORIGINAL_SETTINGS_DEFINITION_ORDER_WITH_NEW_NAMES = OrderedDict([
    ('access_mode', 'AccessModeSetting'),
    ('allow_persistent_secrets', 'AllowPersistentSecrets'),
    ('catalog_error_max_schemas', 'CatalogErrorMaxSchema'),
    ('checkpoint_threshold', 'CheckpointThresholdSetting'),
    ('debug_checkpoint_abort', 'DebugCheckpointAbort'),
    ('debug_force_external', 'DebugForceExternal'),
    ('debug_force_no_cross_product', 'DebugForceNoCrossProduct'),
    ('debug_skip_checkpoint_on_commit', 'DebugSkipCheckpointOnCommit'),
    ('ordered_aggregate_threshold', 'OrderedAggregateThreshold'),
    ('debug_asof_iejoin', 'DebugAsOfIEJoin'),
    ('prefer_range_joins', 'PreferRangeJoins'),
    ('debug_window_mode', 'DebugWindowMode'),
    ('default_collation', 'DefaultCollationSetting'),
    ('default_order', 'DefaultOrderSetting'),
    ('default_null_order', 'DefaultNullOrderSetting'),
    ('default_secret_storage', 'DefaultSecretStorage'),
    ('disabled_filesystems', 'DisabledFileSystemsSetting'),
    ('disabled_optimizers', 'DisabledOptimizersSetting'),
    ('enable_external_access', 'EnableExternalAccessSetting'),
    ('enable_macro_dependencies', 'EnableMacrosDependencies'),
    ('enable_view_dependencies', 'EnableViewDependencies'),
    ('enable_fsst_vectors', 'EnableFSSTVectors'),
    ('allow_unsigned_extensions', 'AllowUnsignedExtensionsSetting'),
    ('allow_community_extensions', 'AllowCommunityExtensionsSetting'),
    ('allow_extensions_metadata_mismatch', 'AllowExtensionsMetadataMismatchSetting'),
    ('allow_unredacted_secrets', 'AllowUnredactedSecretsSetting'),
    ('custom_extension_repository', 'CustomExtensionRepository'),
    ('autoinstall_extension_repository', 'AutoloadExtensionRepository'),
    ('autoinstall_known_extensions', 'AutoinstallKnownExtensions'),
    ('autoload_known_extensions', 'AutoloadKnownExtensions'),
    ('enable_object_cache', 'EnableObjectCacheSetting'),
    ('storage_compatibility_version', 'StorageCompatibilityVersion'),
    ('enable_http_metadata_cache', 'EnableHTTPMetadataCacheSetting'),
    ('enable_profiling', 'EnableProfilingSetting'),
    ('custom_profiling_settings', 'CustomProfilingSettings'),
    ('enable_progress_bar', 'EnableProgressBarSetting'),
    ('enable_progress_bar_print', 'EnableProgressBarPrintSetting'),
    ('explain_output', 'ExplainOutputSetting'),
    ('', 'ExportLargeBufferArrow', 'ArrowLargeBufferSizeSetting'),
    ('', 'ExtensionDirectorySetting'),
    ('', 'ExternalThreadsSetting'),
    ('', 'FileSearchPathSetting'),
    ('', 'ForceCompressionSetting'),
    ('', 'ForceBitpackingModeSetting'),
    ('', 'HomeDirectorySetting'),
    ('', 'HTTPProxy', 'HttpProxySetting'),
    ('', 'HTTPProxyUsername', 'HttpProxyUsernameSetting'),
    ('', 'HTTPProxyPassword', 'HttpProxyPasswordSetting'),
    ('', 'IntegerDivisionSetting'),
    ('', 'LogQueryPathSetting'),
    ('', 'LockConfigurationSetting'),
    ('', 'IEEEFloatingPointOpsSetting'),
    ('', 'ImmediateTransactionModeSetting'),
    ('', 'MaximumExpressionDepthSetting', 'MaxExpressionDepthSetting'),
    ('', 'MaximumMemorySetting', 'MaxMemorySetting'),
    ('', 'StreamingBufferSize', 'StreamingBufferSizeSetting'),
    ('', 'MaximumTempDirectorySize', 'MaxTempDirectorySizeSetting'),
    ('', 'MaximumVacuumTasks', 'MaxVacuumTasksSetting'),
    ('', 'MergeJoinThreshold', 'MergeJoinThresholdSetting'),
    ('', 'NestedLoopJoinThreshold', 'NestedLoopJoinThresholdSetting'),
    ('', 'OldImplicitCasting', 'OldImplicitCastingSetting'),
    ('', 'OrderByNonIntegerLiteral', 'OrderByNonIntegerLiteralSetting'),
    ('', 'PartitionedWriteFlushThreshold', 'PartitionedWriteFlushThresholdSetting'),
    ('', 'PartitionedWriteMaxOpenFiles', 'PartitionedWriteMaxOpenFilesSetting'),
    ('', 'DefaultBlockAllocSize', 'DefaultBlockSizeSetting'),
    ('', 'IndexScanPercentage', 'IndexScanPercentageSetting'),
    ('', 'IndexScanMaxCount', 'IndexScanMaxCountSetting'),
    ('', 'PasswordSetting'),
    ('', 'PerfectHashThresholdSetting', 'PerfectHtThresholdSetting'),
    ('', 'PivotFilterThreshold', 'PivotFilterThresholdSetting'),
    ('', 'PivotLimitSetting'),
    ('', 'PreserveIdentifierCase', 'PreserveIdentifierCaseSetting'),
    ('', 'PreserveInsertionOrder', 'PreserveInsertionOrderSetting'),
    ('', 'ArrowOutputListView', 'ArrowOutputListViewSetting'),
    ('', 'LosslessConversionArrow', 'ArrowLosslessConversionSetting'),
    ('', 'ProduceArrowStringView', 'ProduceArrowStringViewSetting'),
    ('', 'ProfileOutputSetting'),
    ('', 'ProfilingModeSetting'),
    ('', 'ProgressBarTimeSetting'),
    ('', 'ScalarSubqueryErrorOnMultipleRows', 'ScalarSubqueryErrorOnMultipleRowsSetting'),
    ('', 'SchemaSetting'),
    ('', 'SearchPathSetting'),
    ('', 'SecretDirectorySetting'),
    ('', 'TempDirectorySetting'),
    ('', 'ThreadsSetting'),
    ('', 'UsernameSetting'),
    ('', 'AllocatorFlushThreshold', 'AllocatorFlushThresholdSetting'),
    ('', 'AllocatorBulkDeallocationFlushThreshold', 'AllocatorBulkDeallocationFlushThresholdSetting'),
    ('', 'AllocatorBackgroundThreadsSetting'),
    ('', 'DuckDBApiSetting'),
    ('', 'CustomUserAgentSetting'),
    ('', 'EnableHTTPLoggingSetting'),
    ('', 'HTTPLoggingOutputSetting', '')
])

# markers
START_MARKER = (
    f"//===----------------------------------------------------------------------===//\n"
    f"// This code is autogenerated from 'update_settings_header_file.py'.\n"
    f"// Please do not make any changes directly here, as they will be overwritten.\n//\n"
    f"// Start of the auto-generated list of settings structures\n"
    f"//===----------------------------------------------------------------------===//\n"
)
END_MARKER = "// End of the auto-generated list of settings structures"


def extract_declarations(setting) -> str:
    definition = (
        f"struct {setting.struct_name} {{\n"
        f"    using RETURN_TYPE = {setting.type};\n"
        f"    static constexpr const char *Name = \"{setting.name}\";\n"
        f"    static constexpr const char *Description = \"{setting.description}\";\n"
        f"    static constexpr const LogicalTypeId InputType = LogicalTypeId::{setting.sql_type};\n"
    )
    if setting.scope == "GLOBAL" or setting.scope == "GLOBAL_LOCAL":
        definition += f"    static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);\n"
        definition += f"    static void ResetGlobal(DatabaseInstance *db, DBConfig &config);\n"
        if setting.add_verification_in_SET:
            definition += (
                f"static bool VerifySet(DatabaseInstance *db, DBConfig &config, const Value &input);\n"
            )
        if setting.add_verification_in_RESET:
            definition += f"static bool VerifyDBInstanceRESET(DatabaseInstance *db, DBConfig &config);\n"
    if setting.scope == "LOCAL" or setting.scope == "GLOBAL_LOCAL":
        definition += f"    static void SetLocal(ClientContext &context, const Value &parameter);\n"
        definition += f"    static void ResetLocal(ClientContext &context);\n"
        if setting.add_verification_in_SET:
            definition += f"static bool VerifySet(ClientContext &context, const Value &input);\n"
        if setting.add_verification_in_RESET:
            definition += f"static bool VerifyDBInstanceRESET(ClientContext &context);\n"

    definition += f"    static Value GetSetting(const ClientContext &context);\n"
    definition += f"}};\n\n"
    return definition

def reorder_settings():
    reordered_list = []
    original_ordered = ORIGINAL_SETTINGS_DEFINITION_ORDER_WITH_NEW_NAMES
    index = 0

    # check which original settings exist in the settings list
    for original_setting in original_ordered:
        for setting in SettingsList:
            if original_ordered[original_setting] in setting.struct_name:
                reordered_list.append(original_ordered[original_setting])
            else:
                reordered_list.append('')

    # add any new settings that were not included in the original order
    for setting in SettingsList:
        if setting.struct_name not in reordered_list and setting not in original_ordered:
            reordered_list.append(setting)

    return reordered_list


# generate code for all the settings for the the header file
def generate_content(header_file_path):
    with open(header_file_path, 'r') as source_file:
        source_code = source_file.read()

    # find start and end indexes of the auto-generated section
    start_index, end_index = find_start_end_indexes(source_code, START_MARKER, END_MARKER, header_file_path)

    # split source code into sections
    start_section = source_code[: start_index + 1]
    end_section = SEPARATOR + source_code[end_index:]

    reorder_settings()
    new_content = "".join(extract_declarations(setting) for setting in SettingsList)
    return start_section + new_content + end_section


def generate():
    from .config import DUCKDB_SETTINGS_HEADER_FILE

    print(f"Updating {DUCKDB_SETTINGS_HEADER_FILE}")
    new_content = generate_content(DUCKDB_SETTINGS_HEADER_FILE)
    write_content_to_file(new_content, DUCKDB_SETTINGS_HEADER_FILE)


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")
