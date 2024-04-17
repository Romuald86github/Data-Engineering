from google.oauth2 import service_account # type: ignore
from googleapiclient.discovery import build # type: ignore

def update_data_studio_report(loan_df, payment_df, on_time_data, loan_delay_data, total_loan_data, monthly_repayment_data, loan_default_data):
    # Authenticate with Google Data Studio API
    credentials = service_account.Credentials.from_service_account_file(
        'path_to_service_account.json',
        scopes=["https://www.googleapis.com/auth/datastudio"]
    )
    service = build('datastudio', 'v1', credentials=credentials)

    # Prepare the data for updating the report
    updated_data = {
        "data": {
            "dataSets": [
                {
                    "dataSetId": "your_data_set_id",
                    "tables": [
                        {
                            "tableId": "your_table_id",
                            "rows": loan_df.to_dict(orient="records")
                        },
                        {
                            "tableId": "your_payment_table_id",
                            "rows": payment_df.to_dict(orient="records")
                        }
                    ]
                }
            ]
        }
    }

    # Make the API request to update the report
    service.reports().data().update(
        body=updated_data,
        name=f'reports/your_report_id',
        datasetId="your_dataset_id"
    ).execute()

    # Continue with updating the report with the charts
    chart_updates = [
        {
            "id": "chart_1",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "PaidOnTime",
                    "metric": on_time_data.to_dict()
                }
            }
        },
        {
            "id": "chart_2",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "LateTime",
                    "metric": loan_delay_data.to_dict()
                }
            }
        },
        {
            "id": "chart_3",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "DefaultStatus",
                    "metric": total_loan_data.to_dict()
                }
            }
        },
        {
            "id": "chart_4",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "BAR",
                "fields": {
                    "dimension": "PaidMonth",
                    "metric": monthly_repayment_data.to_dict()
                }
            }
        },
        {
            "id": "chart_5",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "DefaultStatus",
                    "metric": loan_default_data.to_dict()
                }
            }
        }
    ]

    # Update each chart in the report
    for chart in chart_updates:
        service.reports().components().patch(
            name=f'reports/your_report_id/components/{chart["id"]}',
            body=chart
        ).execute()
