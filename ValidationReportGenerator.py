"""Report generation utilities for database transition validation."""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Template

from data_class.OverallValidationResult import OverallValidationResult
from data_class.ValidationStatus import ValidationStatus
from database_setup.build_database_description import (
    build_database_description,
)


class ValidationReportGenerator:
    """Generate various types of validation reports."""

    def get_file_content(self, filename: str) -> str:
        """Read and return the content of a template file in the same directory as this script."""
        file_path = Path(__file__).parent / filename
        with open(file_path, "r") as f:
            return f.read()

    def __init__(self, settings: Dict[str, Any]):
        """Initialize the report generator."""

        self.settings = settings
        output_dir = (
            settings["validation_settings"]["output_dir"]
            if settings["validation_settings"]
            and settings["validation_settings"]["output_dir"]
            else "database_validation_reports"
        )

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def generate_all_reports(
        self, result: OverallValidationResult
    ) -> Dict[str, str]:
        """Generate all types of reports and return file paths."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        reports = {}
        reports["json"] = self.generate_json_report(
            result, f"validation_report_{timestamp}.json"
        )
        reports["csv"] = self.generate_csv_report(
            result, f"validation_report_{timestamp}.csv"
        )
        reports["html"] = self.generate_html_report(
            result, f"validation_report_{timestamp}.html"
        )
        reports["summary"] = self.generate_summary_report(
            result, f"validation_summary_{timestamp}.txt"
        )

        # Print summary
        print("\n" + "=" * 60)
        print("VALIDATION COMPLETE")
        print("=" * 60)
        print(f"Overall Status: {result.overall_status_table_result.value}")
        print(f"Total Tables: {result.summary_stats['total_tables']}")
        print(
            f"Successful Tables: {result.summary_stats['successful_tables']}"
        )
        print(f"Failed Tables: {result.summary_stats['failed_tables']}")
        print(f"Warning Tables: {result.summary_stats['warning_tables']}")
        print(
            f"Table Success Rate: {result.summary_stats['success_rate_tables']:.2f}%"
        )
        print(
            f"Data Success Rate: {result.summary_stats['overall_data_success_rate']:.2f}%"
        )
        print(f"Execution Time: {result.total_execution_time:.2f} seconds")

        print("\nReports Generated:")
        for report_type, filepath in reports.items():
            print(f"  {report_type.upper()}: {filepath}")

        return reports

    def generate_json_report(
        self, result: OverallValidationResult, filename: str
    ) -> str:
        """Generate detailed JSON report."""
        filepath = self.output_dir / filename

        # Convert result to dict for JSON serialization
        report_data = {
            "validation_id": result.validation_id,
            "source_database": f"{result.settings['database_setting']['source_database']['type']} ({result.settings['database_setting']['source_database']['schema']})",
            "target_database": f"{result.settings['database_setting']['target_database']['type']} ({result.settings['database_setting']['target_database']['schema']})",
            "start_time": result.start_time.isoformat(),
            "end_time": (
                result.end_time.isoformat() if result.end_time else None
            ),
            "overall_status": result.overall_status_table_result.value,
            "total_execution_time": result.total_execution_time,
            "summary_stats": result.summary_stats,
            "data_match_validation_result": [],
            "schema_validation_results": [],
        }

        # Add table results
        for table_result in result.data_match_validation_result:
            table_data = {
                "table_name": table_result.table_name,
                "source_table": table_result.source_table,
                "target_table": table_result.target_table,
                "status": table_result.status.value,
                "source_count": table_result.source_count,
                "target_count": table_result.target_count,
                "percent_count_difference": table_result.percent_count_difference,
                "matching_records": table_result.matching_records,
                "success_rate": table_result.success_rate,
                "execution_time_seconds": table_result.execution_time_seconds,
                "validation_issues": [
                    {
                        "issue_type": issue.issue_type,
                        "description": issue.description,
                        "severity": issue.severity.value,
                        "source_value": issue.source_value,
                        "target_value": issue.target_value,
                        "additional_info": issue.additional_info,
                    }
                    for issue in table_result.data_match_validation_issues
                ],
            }
            report_data["data_match_validation_result"].append(table_data)

        # Add schema results
        for schema_result in result.schema_validation_results:
            schema_data = {
                "source_table_name": schema_result.source_table_name,
                "target_table_name": schema_result.target_table_name,
                "status": schema_result.status.value,
                "source_col_names": schema_result.source_col_names,
                "target_col_names": schema_result.target_col_names,
                "missing_columns": schema_result.missing_columns,
                "extra_columns": schema_result.extra_columns,
                "type_mismatches": schema_result.type_mismatches,
                "validation_issues": [
                    {
                        "issue_type": issue.issue_type,
                        "description": issue.description,
                        "severity": issue.severity.value,
                    }
                    for issue in schema_result.validation_issues
                ],
            }
            report_data["schema_validation_results"].append(schema_data)

        with open(filepath, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        return str(filepath)

    def generate_csv_report(
        self, result: OverallValidationResult, filename: str
    ) -> str:
        """Generate CSV report with table-level results."""
        filepath = self.output_dir / filename

        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow(
                [
                    "Table Name",
                    "Source Table",
                    "Target Table",
                    "Status",
                    "Source Count",
                    "Target Count",
                    "Percent Count Difference",
                    "Matching Records",
                    "Success Rate (%)",
                    "Execution Time (s)",
                    "Issues",
                ]
            )

            # Write data rows
            for table_result in result.data_match_validation_result:
                issues_summary = "; ".join(
                    [
                        f"{issue.issue_type}: {issue.description}"
                        for issue in table_result.data_match_validation_issues
                    ]
                )

                writer.writerow(
                    [
                        table_result.table_name,
                        table_result.source_table,
                        table_result.target_table,
                        table_result.status.value,
                        table_result.source_count,
                        table_result.target_count,
                        f"{table_result.percent_count_difference:.2f}",
                        table_result.matching_records,
                        f"{table_result.success_rate:.2f}",
                        f"{table_result.execution_time_seconds:.2f}",
                        issues_summary,
                    ]
                )

        return str(filepath)

    def generate_html_report(
        self, result: OverallValidationResult, filename: str
    ) -> str:
        """Generate HTML report with dashboard-style layout."""
        filepath = self.output_dir / filename

        # Load template contents using reusable method
        html_report_template_folder = "html_report_template"
        html_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template.html"
        )
        html_template = Template(html_template_content)

        template_css_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template.css"
        )
        template_script_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template.js"
        )

        venn_diagram_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2.html"
        )
        venn_diagram_template = Template(venn_diagram_template_content)

        venn_diagram_set1_in_set2_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_set1_in_set2.html"
        )
        venn_diagram_set1_in_set2_template = Template(
            venn_diagram_set1_in_set2_template_content
        )

        venn_diagram_set2_in_set1_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_set2_in_set1.html"
        )
        venn_diagram_set2_in_set1_template = Template(
            venn_diagram_set2_in_set1_template_content
        )

        venn_diagram_html = {}

        venn_diagram_db1_all_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_DB1 All.html"
        )
        venn_diagram_db1_all_template = Template(
            venn_diagram_db1_all_template_content
        )

        venn_diagram_db2_all_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_DB2 All.html"
        )
        venn_diagram_db2_all_template = Template(
            venn_diagram_db2_all_template_content
        )

        venn_diagram_none_match_db1_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_none_match_DB1.html"
        )
        venn_diagram_none_match_db1_template = Template(
            venn_diagram_none_match_db1_template_content
        )

        venn_diagram_none_match_db2_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_none_match_DB2.html"
        )
        venn_diagram_none_match_db2_template = Template(
            venn_diagram_none_match_db2_template_content
        )
        venn_diagram_none_match_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_none_match.html"
        )
        venn_diagram_none_match_template = Template(
            venn_diagram_none_match_template_content
        )

        venn_diagram_sets_match_db1_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_sets_match_DB1.html"
        )
        venn_diagram_sets_match_db1_template = Template(
            venn_diagram_sets_match_db1_template_content
        )

        venn_diagram_sets_match_db2_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_sets_match_DB2.html"
        )
        venn_diagram_sets_match_db2_template = Template(
            venn_diagram_sets_match_db2_template_content
        )
        venn_diagram_sets_match_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_sets_match.html"
        )
        venn_diagram_sets_match_template = Template(
            venn_diagram_sets_match_template_content
        )
        venn_diagram_sets_match_perfect_template_content = self.get_file_content(
            f"{html_report_template_folder}/ValidationReportGenerator_template_venn_diagram_2_sets_match_perfect.html"
        )
        venn_diagram_sets_match_perfect_template = Template(
            venn_diagram_sets_match_perfect_template_content
        )

        for table in result.data_match_validation_result:
            if (
                table.compare_sample_data_result
                and table.compare_sample_data_result.matching_set_record_count
                and table.compare_sample_data_result.source_sample_set_count
                and table.compare_sample_data_result.target_sample_set_count
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    > 0
                )
                and (
                    table.compare_sample_data_result.source_sample_set_count
                    > table.compare_sample_data_result.matching_set_record_count
                )
                and (
                    table.compare_sample_data_result.target_sample_set_count
                    > table.compare_sample_data_result.matching_set_record_count
                )
            ):
                venn_diagram_db1_all_html = ""
                if (
                    table.compare_sample_data_result.source_sample_set_count
                    < table.compare_sample_data_result.source_sample_count
                ):
                    venn_diagram_db1_all_html = venn_diagram_db1_all_template.render(
                        set_count=table.compare_sample_data_result.source_sample_set_count,
                        total_count=table.compare_sample_data_result.source_sample_count,
                    )

                venn_diagram_db2_all_html = ""
                if (
                    table.compare_sample_data_result.target_sample_set_count
                    < table.compare_sample_data_result.target_sample_count
                ):
                    venn_diagram_db2_all_html = venn_diagram_db2_all_template.render(
                        set_count=table.compare_sample_data_result.target_sample_set_count,
                        total_count=table.compare_sample_data_result.target_sample_count,
                    )

                venn_diagram_db_all_html = f" {venn_diagram_db1_all_html}  {venn_diagram_db2_all_html}  "

                venn_diagram_html[table.unique_data_mapping_id] = (
                    venn_diagram_template.render(
                        intersection_value=table.compare_sample_data_result.matching_set_record_count,
                        set1_value=table.compare_sample_data_result.source_sample_set_count,
                        set2_value=table.compare_sample_data_result.target_sample_set_count,
                        set1_label=f"{result.settings['database_setting']['source_database']['schema']}",
                        set2_label=f"{result.settings['database_setting']['target_database']['schema']}",
                        venn_diagram_db_all_html=venn_diagram_db_all_html,
                    )
                )
            elif (
                table.compare_sample_data_result
                and table.compare_sample_data_result.matching_set_record_count
                and table.compare_sample_data_result.source_sample_set_count
                and table.compare_sample_data_result.target_sample_set_count
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    > 0
                )
                and (
                    table.compare_sample_data_result.source_sample_set_count
                    == table.compare_sample_data_result.matching_set_record_count
                )
                and (
                    table.compare_sample_data_result.target_sample_set_count
                    > table.compare_sample_data_result.matching_set_record_count
                )
            ):
                venn_diagram_html[table.unique_data_mapping_id] = (
                    venn_diagram_set1_in_set2_template.render(
                        set1_value=table.compare_sample_data_result.source_sample_set_count,
                        set2_value=table.compare_sample_data_result.target_sample_set_count,
                        set1_label=f"{result.settings['database_setting']['source_database']['schema']}",
                        set2_label=f"{result.settings['database_setting']['target_database']['schema']}",
                    )
                )

            elif (
                table.compare_sample_data_result
                and table.compare_sample_data_result.matching_set_record_count
                and table.compare_sample_data_result.source_sample_set_count
                and table.compare_sample_data_result.target_sample_set_count
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    > 0
                )
                and (
                    table.compare_sample_data_result.source_sample_set_count
                    > table.compare_sample_data_result.matching_set_record_count
                )
                and (
                    table.compare_sample_data_result.target_sample_set_count
                    == table.compare_sample_data_result.matching_set_record_count
                )
            ):
                venn_diagram_html[table.unique_data_mapping_id] = (
                    venn_diagram_set2_in_set1_template.render(
                        set1_value=table.compare_sample_data_result.source_sample_set_count,
                        set2_value=table.compare_sample_data_result.target_sample_set_count,
                        set1_label=f"{result.settings['database_setting']['source_database']['schema']}",
                        set2_label=f"{result.settings['database_setting']['target_database']['schema']}",
                    )
                )
            elif (
                table.compare_sample_data_result
                and table.compare_sample_data_result.source_sample_set_count
                and table.compare_sample_data_result.target_sample_set_count
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    == 0
                )
                and (
                    table.compare_sample_data_result.source_sample_set_count
                    > 0
                )
                and (
                    table.compare_sample_data_result.target_sample_set_count
                    > 0
                )
            ):
                venn_diagram_none_match_db1_html = ""
                if (
                    table.compare_sample_data_result.source_sample_set_count
                    < table.compare_sample_data_result.source_sample_count
                ):
                    venn_diagram_none_match_db1_html = venn_diagram_none_match_db1_template.render(
                        set_count=table.compare_sample_data_result.source_sample_set_count,
                        total_count=table.compare_sample_data_result.source_sample_count,
                    )

                venn_diagram_none_match_db2_html = ""
                if (
                    table.compare_sample_data_result.target_sample_set_count
                    < table.compare_sample_data_result.target_sample_count
                ):
                    venn_diagram_none_match_db2_html = venn_diagram_none_match_db2_template.render(
                        set_count=table.compare_sample_data_result.target_sample_set_count,
                        total_count=table.compare_sample_data_result.target_sample_count,
                    )

                venn_diagram_db_all_html = f" {venn_diagram_none_match_db1_html}  {venn_diagram_none_match_db2_html}  "

                venn_diagram_html[table.unique_data_mapping_id] = (
                    venn_diagram_none_match_template.render(
                        set1_value=table.compare_sample_data_result.source_sample_set_count,
                        set2_value=table.compare_sample_data_result.target_sample_set_count,
                        set1_label=f"{result.settings['database_setting']['source_database']['schema']}",
                        set2_label=f"{result.settings['database_setting']['target_database']['schema']}",
                        venn_diagram_db_all_html=venn_diagram_db_all_html,
                    )
                )
            elif (
                table.compare_sample_data_result
                and table.compare_sample_data_result.source_sample_set_count
                and table.compare_sample_data_result.target_sample_set_count
                and table.compare_sample_data_result.matching_set_record_count
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    == table.compare_sample_data_result.source_sample_set_count
                )
                and (
                    table.compare_sample_data_result.matching_set_record_count
                    == table.compare_sample_data_result.target_sample_set_count
                )
            ):
                venn_diagram_sets_match_db1_html = ""
                if (
                    table.compare_sample_data_result.source_sample_count
                    > table.compare_sample_data_result.source_sample_set_count
                ):
                    venn_diagram_sets_match_db1_html = venn_diagram_sets_match_db1_template.render(
                        set_count=table.compare_sample_data_result.source_sample_set_count,
                        total_count=table.compare_sample_data_result.source_sample_count,
                    )

                venn_diagram_sets_match_db2_html = ""
                if (
                    table.compare_sample_data_result.target_sample_count
                    > table.compare_sample_data_result.target_sample_set_count
                ):
                    venn_diagram_sets_match_db2_html = venn_diagram_sets_match_db2_template.render(
                        set_count=table.compare_sample_data_result.target_sample_set_count,
                        total_count=table.compare_sample_data_result.target_sample_count,
                    )

                if (
                    table.compare_sample_data_result.source_sample_count
                    == table.compare_sample_data_result.source_sample_set_count
                ) and (
                    table.compare_sample_data_result.target_sample_count
                    == table.compare_sample_data_result.target_sample_set_count
                ):
                    venn_diagram_sets_match_perfect_html = (
                        venn_diagram_sets_match_perfect_template.render(
                            key_columns=table.key_columns,
                        )
                    )
                    venn_diagram_db_all_html = (
                        venn_diagram_sets_match_perfect_html
                    )
                else:
                    venn_diagram_db_all_html = f" {venn_diagram_sets_match_db1_html}  {venn_diagram_sets_match_db2_html}  "

                venn_diagram_html[table.unique_data_mapping_id] = (
                    venn_diagram_sets_match_template.render(
                        set1_value=table.compare_sample_data_result.source_sample_set_count,
                        set1_label=f"{result.settings['database_setting']['source_database']['schema']}",
                        set2_label=f"{result.settings['database_setting']['target_database']['schema']}",
                        venn_diagram_db_all_html=venn_diagram_db_all_html,
                    )
                )

        source_database = build_database_description(
            result.settings["database_setting"]["source_database"]
        )
        target_database = build_database_description(
            result.settings["database_setting"]["target_database"]
        )

        html_content = html_template.render(
            validation_id=result.validation_id,
            source_database=source_database,
            target_database=target_database,
            start_time=result.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=(
                result.end_time.strftime("%Y-%m-%d %H:%M:%S")
                if result.end_time
                else "N/A"
            ),
            execution_time=result.total_execution_time,
            overall_status=result.overall_status.value,
            overall_status_table_result=result.overall_status_table_result.value,
            overall_status_schema_result=result.overall_status_schema_result.value,
            overall_status_data_match_result=result.overall_status_data_match_result.value,
            overall_status_row_count_result=result.overall_status_row_count_result.value,
            overall_status_rule_based_data_validation_result=result.overall_status_rule_based_data_validation_result.value,
            overall_status_distribution_based_data_validation_result="PASSED",
            summary=result.summary_stats,
            data_match_validation_result=result.data_match_validation_result,
            data_match_validation_result_grouped=result.data_match_validation_result_grouped,
            schema_validation_results=result.schema_validation_results,
            disable_rule_based_data_validation=(
                not isinstance(
                    self.settings.get("validation_settings", {}), dict
                )
                or not self.settings["validation_settings"].get(
                    "enable_rule_based_data_validation", True
                )
            ),
            disable_distribution_based_data_validation=(
                not isinstance(
                    self.settings.get("validation_settings", {}), dict
                )
                or not self.settings["validation_settings"].get(
                    "enable_distribution_based_data_validation", True
                )
            ),
            report_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            template_css_content=template_css_content,
            template_script_content=template_script_content,
            venn_diagram_html=venn_diagram_html,
        )

        with open(filepath, "w") as f:
            f.write(html_content)

        return str(filepath)

    def generate_summary_report(
        self, result: OverallValidationResult, filename: str
    ) -> str:
        """Generate concise text summary report."""
        filepath = self.output_dir / filename

        summary = result.summary_stats

        with open(filepath, "w") as f:
            f.write("DATABASE TRANSITION VALIDATION SUMMARY\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Validation ID: {result.validation_id}\n")

            f.write(
                f"Source Database: {result.settings['database_setting']['source_database']['type']} ({result.settings['database_setting']['source_database']['schema']})\n"
            )

            f.write(
                f"Target Database: {result.settings['database_setting']['target_database']['type']} ({result.settings['database_setting']['target_database']['schema']})\n"
            )

            f.write(
                f"Overall Status: {result.overall_status_table_result.value}\n"
            )
            f.write(
                f"Execution Time: {result.total_execution_time:.2f} seconds\n\n"
            )

            f.write("TABLES SUMMARY:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Tables: {summary['total_tables']}\n")
            f.write(f"Successful: {summary['successful_tables']}\n")
            f.write(f"Failed: {summary['failed_tables']}\n")
            f.write(f"Warnings: {summary['warning_tables']}\n")
            f.write(f"Success Rate: {summary['success_rate_tables']:.2f}%\n\n")

            f.write("DATA SUMMARY:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Source Records: {summary['total_source_records']:,}\n")
            f.write(f"Target Records: {summary['total_target_records']:,}\n")
            f.write(
                f"Matching Records: {summary['total_matching_records']:,}\n"
            )
            f.write(
                f"Data Success Rate: {summary['overall_data_success_rate']:.2f}%\n\n"
            )

            # Failed tables details
            failed_tables = [
                r
                for r in result.data_match_validation_result
                if r.status == ValidationStatus.FAIL
            ]
            if failed_tables:
                f.write("FAILED TABLES:\n")
                f.write("-" * 20 + "\n")
                for table in failed_tables:
                    f.write(
                        f"- {table.table_name}: {table.success_rate:.2f}% success rate\n"
                    )
                f.write("\n")

            # Warning tables details
            warning_tables = [
                r
                for r in result.data_match_validation_result
                if r.status == ValidationStatus.WARNING
            ]
            if warning_tables:
                f.write("WARNING TABLES:\n")
                f.write("-" * 20 + "\n")
                for table in warning_tables:
                    f.write(
                        f"- {table.table_name}: {table.success_rate:.2f}% success rate\n"
                    )
                f.write("\n")

        return str(filepath)
