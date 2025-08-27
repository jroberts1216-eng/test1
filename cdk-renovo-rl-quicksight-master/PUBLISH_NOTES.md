```
I copied the example sales pipeline dashboard analysis and dataset so I have sales-pipeline-example-analysis analysis and sales-pipeline-example dataset:
- Ran the following to create a template
aws quicksight create-template --aws-account-id 719035735300 --template-id sales-pipeline-example --name sales-pipeline-example --source-entity '{"SourceAnalysis": {"Arn": "arn:aws:quicksight:us-east-1:719035735300:analysis/sales-pipeline-example-analysis", "DataSetReferences": [{"DataSetPlaceholder": "SalesData", "DataSetArn": "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"}]}}' --profile services-admin
{
    "Status": 202,
    "Arn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example",
    "VersionArn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example/version/1",
    "TemplateId": "sales-pipeline-example",
    "CreationStatus": "CREATION_IN_PROGRESS",
    "RequestId": "c79fd45b-4bde-44c2-ae5a-ad104c3972bd"
}
- Created a dashboard with the following
aws quicksight create-dashboard --aws-account-id 719035735300 --dashboard-id sales-pipeline-example --name sales-pipeline-example --source-entity '{"SourceTemplate": {"Arn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example/version/1", "DataSetReferences": [{"DataSetPlaceholder": "SalesData","DataSetArn": "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"}]}}' --profile services-admin
{
    "Status": 202,
    "Arn": "arn:aws:quicksight:us-east-1:719035735300:dashboard/sales-pipeline-example",
    "VersionArn": "arn:aws:quicksight:us-east-1:719035735300:dashboard/sales-pipeline-example/version/1",
    "DashboardId": "sales-pipeline-example",
    "CreationStatus": "CREATION_IN_PROGRESS",
    "RequestId": "2cefd5fe-8205-482c-9a95-96f0b9442287"
}
- Dashboard is visible and looks as expected
- I next described my existing analysis to get the saved timestamp
aws quicksight describe-analysis --aws-account-id 719035735300 --analysis-id sales-pipeline-example-analysis --profile services-admin
{
    "Status": 200,
    "Analysis": {
        "AnalysisId": "sales-pipeline-example-analysis",
        "Arn": "arn:aws:quicksight:us-east-1:719035735300:analysis/sales-pipeline-example-analysis",
        "Name": "sales-pipeline-example",
        "Status": "UPDATE_SUCCESSFUL",
        "DataSetArns": [
            "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"
        ],
        "CreatedTime": "2024-11-19T11:20:02.918000-05:00",
        "LastUpdatedTime": "2024-11-27T14:22:58.618000-05:00",
        "Sheets": [
            {
                "SheetId": "e3998bcd-ddf1-4890-9a0e-dffdb8ba5bfe",
                "Name": "Sheet 1"
            }
        ]
    },
    "RequestId": "ae5385e3-50fa-462b-99a1-16424848a7eb"
}
- I modified the analysis in the console and duplicated a visual. Autosave is on so I ran the same command to make sure my last updated time changed
aws quicksight describe-analysis --aws-account-id 719035735300 --analysis-id sales-pipeline-example-analysis --profile services-admin
{
    "Status": 200,
    "Analysis": {
        "AnalysisId": "sales-pipeline-example-analysis",
        "Arn": "arn:aws:quicksight:us-east-1:719035735300:analysis/sales-pipeline-example-analysis",
        "Name": "sales-pipeline-example",
        "Status": "UPDATE_SUCCESSFUL",
        "DataSetArns": [
            "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"
        ],
        "CreatedTime": "2024-11-19T11:20:02.918000-05:00",
        "LastUpdatedTime": "2024-11-27T15:22:02.651000-05:00",
        "Sheets": [
            {
                "SheetId": "e3998bcd-ddf1-4890-9a0e-dffdb8ba5bfe",
                "Name": "Sheet 1"
            }
        ]
    },
    "RequestId": "0a3eca7f-3b72-4c92-b2d1-5298741de597"
}
- The time changed so my changes should be propagated to the analysis so next I pulled a new version of my tempalte
aws quicksight update-template --aws-account-id 719035735300 --template-id sales-pipeline-example --name sales-pipeline-example --source-entity '{"SourceAnalysis": {"Arn": "arn:aws:quicksight:us-east-1:719035735300:analysis/sales-pipeline-example-analysis", "DataSetReferences": [{"DataSetPlaceholder": "SalesData", "DataSetArn": "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"}]}}' --profile services-admin
{
    "Status": 202,
    "TemplateId": "sales-pipeline-example",
    "Arn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example",
    "VersionArn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example/version/2",
    "CreationStatus": "CREATION_IN_PROGRESS",
    "RequestId": "6fa7797b-ca0f-4d0f-884e-b1ae9f45c635"
}
- I then updated the dashboard with this new version
aws quicksight update-dashboard --aws-account-id 719035735300 --dashboard-id sales-pipeline-example --name sales-pipeline-example --source-entity '{"SourceTemplate": {"Arn": "arn:aws:quicksight:us-east-1:719035735300:template/sales-pipeline-example/version/2", "DataSetReferences": [{"DataSetPlaceholder": "SalesData","DataSetArn": "arn:aws:quicksight:us-east-1:719035735300:dataset/3c3873d5-18ef-4f87-871b-510f135bb6bb"}]}}' --profile services-admin
{
    "Arn": "arn:aws:quicksight:us-east-1:719035735300:dashboard/sales-pipeline-example",
    "VersionArn": "arn:aws:quicksight:us-east-1:719035735300:dashboard/sales-pipeline-example/version/2",
    "DashboardId": "sales-pipeline-example",
    "CreationStatus": "CREATION_IN_PROGRESS",
    "Status": 202,
    "RequestId": "f43a5cad-6856-4a15-ba65-ac8c416e9bc6"
}
- Finally I updated the dashboard published version
aws quicksight update-dashboard-published-version --aws-account-id 719035735300 --dashboard-id sales-pipeline-example --version-number 2 --profile services-admin
{
    "Status": 200,
    "DashboardId": "sales-pipeline-example",
    "DashboardArn": "arn:aws:quicksight:us-east-1:719035735300:dashboard/sales-pipeline-example",
    "RequestId": "abc25c2d-6828-4ac2-ab90-e5e6d09301ef"
}
```