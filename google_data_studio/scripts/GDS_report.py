import googleapiclient.discovery # type: ignore

def create_report(credentials, service_account_name, project_id, dataset_id, table_id, data_source_id):
    datastudio = googleapiclient.discovery.build('datastudio', 'v1', credentials=credentials)
    
    body = {
        "name": "Loan_Repayment_Analytics",
        "type": "REPORT",
        "credentials": {
            "name": service_account_name,
            "file": "path_to_service_account.json"  # Replace with the path to your service account JSON file
        },
        "dataSource": {
            "name": "bigquery",
            "parameters": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id
            }
        },
        "components": [
            {
                "id": "chart_1",
                "type": "BAR",
                "dataSource": data_source_id,
                "options": {
                    "title": "Loan Repayment Status"
                }
            },
            {
                "id": "chart_2",
                "type": "PIE",
                "dataSource": data_source_id,
                "options": {
                    "title": "On-time Repayment Status"
                }
            },
            {
                "id": "chart_3",
                "type": "PIE",
                "dataSource": data_source_id,
                "options": {
                    "title": "Loan Delay Status"
                }
            },
            {
                "id": "chart_4",
                "type": "PIE",
                "dataSource": data_source_id,
                "options": {
                    "title": "Total Loan Status"
                }
            },
            {
                "id": "chart_5",
                "type": "BAR",
                "dataSource": data_source_id,
                "options": {
                    "title": "Monthly Loan Repayment"
                }
            },
            {
                "id": "chart_6",
                "type": "PIE",
                "dataSource": data_source_id,
                "options": {
                    "title": "Loan Default Status"
                }
            }
        ]
    }
    
    request = datastudio.reports().create(body=body)
    response = request.execute()
    
    return response
