# Store Status Report Generator
This is a simple Django Web application that accounts for a pre-defined database population code and works with real-time data. Populated data was provided by Loop as part of an interview assignment.

## Endpoints
1. `/trigger_report` endpoint that triggers report generation from the data provided (stored in DB)
    
    1. No input 
    
    2. Output - report_id (random string) 
    
    3. report_id will be used for polling the status of report completion

2. `/get_report` endpoint that returns the status of the report or the csv
    1. Input - report_id
    2. Output
        - if report generation is not complete, return “Running” as the output
        - if report generation is complete, return “Complete” along with the CSV file with the schema described above.