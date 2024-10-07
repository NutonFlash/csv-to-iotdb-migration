Summary of Test Cases
File Name	Test Case Description
csv_unix_timestamp_valid.csv	Valid Unix timestamps
csv_iso_timestamp_valid.csv	Valid ISO 8601 timestamps
csv_missing_timestamp.csv	Rows with missing timestamps
csv_invalid_timestamp.csv	Rows with invalid timestamp formats
csv_extra_columns.csv	Rows with extra, unexpected columns
csv_missing_columns.csv	Rows missing some defined columns
csv_null_values.csv	Rows containing null or empty values
csv_different_delimiters.csv	Rows using a different delimiter (e.g., semicolon)
csv_quoted_fields.csv	Fields enclosed in quotes with escaped characters
csv_inconsistent_columns.csv	Rows with varying numbers of columns
csv_non_numeric_values.csv	Numeric fields containing non-numeric strings
csv_large_values.csv	Rows with extremely large numeric values
csv_duplicate_timestamps.csv	Multiple rows sharing the same timestamp
csv_unexpected_nulls.csv	Mandatory fields unexpectedly null
csv_special_characters.csv	Fields containing special characters, commas, quotes, newlines
csv_mixed_time_formats.csv	Rows using different timestamp formats within the same column
csv_empty_file.csv	Completely empty CSV file
csv_only_headers.csv	CSV file containing only headers and no data
csv_large_batch.csv	Large number of rows to test performance and memory management
csv_non_utf8_encoding.csv	CSV file saved with non-UTF-8 encoding (e.g., ISO-8859-1)
csv_multiline_fields.csv	Fields spanning multiple lines within quoted text
csv_invalid_numeric_types.csv	Numeric fields containing invalid (non-numeric) strings
csv_unexpected_delimiters.csv	Rows using unexpected delimiters
csv_inconsistent_time_formats.csv	Rows with inconsistent time formats in the same column
csv_large_string_fields.csv	Rows with extremely long string fields
csv_with_comments.csv	CSV containing comment lines
csv_with_header_mismatch.csv	CSV headers that do not match the expected schema
csv_with_empty_fields.csv	Rows with empty fields
csv_non_standard_headers.csv	Headers with spaces, special characters, or varying cases
csv_with_duplicate_headers.csv	CSV with duplicated headers causing ambiguity