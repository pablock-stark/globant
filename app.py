from flask import Flask
from flask_restful import Resource, Api
import pandas as pd
import boto3
from google.cloud import bigquery

app = Flask(__name__)
api = Api(app)

class UpdateDepartments(Resource):
    def get(self):
        s3 = boto3.client('s3')
        s3.download_file('globant-202305', 'departments.csv','files/departments.csv')
        df = pd.read_csv('files/departments.csv', header=None)  # read CSV
        df.columns = ['id', 'department']

        JOB_CONFIG = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField("department", bigquery.enums.SqlTypeNames.STRING)
                ],
                write_disposition="WRITE_TRUNCATE")
        
        bq_tbl = 'thinkqwant.globant_tech_test.departments'
        client = bigquery.Client()

        try:
            job = client.load_table_from_dataframe(df, bq_tbl , job_config=JOB_CONFIG)  # Make an API request.
            job.result() 
            return {'Status':'Departments table Updated'}, 200  # return data and 200 OK code
        except:
            return {'Status':'Processing ERROR'}, 500 # return error
    
class UpdateHiredEmployees(Resource):
    def get(self):
        s3 = boto3.client('s3')
        s3.download_file('globant-202305', 'hired_employees.csv','files/hired_employees.csv')
        df = pd.read_csv('files/hired_employees.csv', header=None)  # read CSV
        df.columns = ['id', 'name','datetime','department_id','job_id']
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%dT%H:%M:%S.%f")

        JOB_CONFIG = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("datetime", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("department_id", bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.INT64)
                ],
                write_disposition="WRITE_TRUNCATE")
        
        bq_tbl = 'thinkqwant.globant_tech_test.hired_employees'
        client = bigquery.Client()
        try:
            job = client.load_table_from_dataframe(df, bq_tbl , job_config=JOB_CONFIG)  
            job.result()
            return {'Status':'hired_employees table Updated'}, 200
        except:
            return {'Status':'Processing ERROR'}, 500 
    
class UpdateJobs(Resource):
    def get(self):
        s3 = boto3.client('s3')
        s3.download_file('globant-202305', 'jobs.csv','files/jobs.csv')
        df = pd.read_csv('files/jobs.csv', header=None)  
        df.columns = ['id', 'job']
        JOB_CONFIG = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField("job", bigquery.enums.SqlTypeNames.STRING)
                ],
                write_disposition="WRITE_TRUNCATE")       
        bq_tbl = 'thinkqwant.globant_tech_test.jobs'
        client = bigquery.Client()
        try:
            job = client.load_table_from_dataframe(df, bq_tbl , job_config=JOB_CONFIG)  
            job.result()  
            return {'Status':'jobs table Updated'}, 200  
        except:
            return {'Status':'Processing ERROR'}, 500
        
class ReportEmployeesHired(Resource):
    def get(self):
        query="""
                WITH cte as(
                SELECT
                    deps.department,jobs.job,
                    extract(quarter from datetime) q
                    
                FROM `thinkqwant.globant_tech_test.hired_employees` emp
                JOIN `thinkqwant.globant_tech_test.departments` deps on emp.department_id=deps.id
                JOIN `thinkqwant.globant_tech_test.jobs` jobs on emp.job_id=jobs.id
                WHERE extract(year from emp.datetime)=2021
                ),
                cte_dj as
                (select distinct deps.department,jobs.job
                FROM `thinkqwant.globant_tech_test.hired_employees` emp
                JOIN `thinkqwant.globant_tech_test.departments` deps on emp.department_id=deps.id
                JOIN `thinkqwant.globant_tech_test.jobs` jobs on emp.job_id=jobs.id
                WHERE extract(year from emp.datetime)=2021
                )
                select cte_dj.department,cte_dj.job,
                    coalesce(sq1.Q1,0) Q1,
                    coalesce(sq2.Q2,0) Q2,
                    coalesce(sq3.Q3,0) Q3,
                    coalesce(sq4.Q4,0) Q4
                    
                from cte_dj
                left join
                (select department,job, count(*) Q1
                from cte
                where q=1
                group by department,job
                )sq1 on cte_dj.department=sq1.department and cte_dj.job=sq1.job
                left join
                (
                select department,job, count(*) Q2
                from cte
                where q=2
                group by department,job
                )sq2 on cte_dj.department=sq2.department and cte_dj.job=sq2.job
                left join
                (
                select department,job, count(*) Q3
                from cte
                where q=3
                group by department,job
                )sq3 on cte_dj.department=sq3.department and cte_dj.job=sq3.job
                left join
                (
                select department,job, count(*) Q4
                from cte
                where q=4
                group by department,job
                )sq4 on cte_dj.department=sq4.department and cte_dj.job=sq4.job

                order by department,job
            """
        try:
            client = bigquery.Client()
            bigquery_job = client.query(query)
            df=bigquery_job.result().to_dataframe()
            df = df.reset_index(drop=True)
            response=df.to_json()
            return response
        except:
            return {'Status':'Processing ERROR'}, 500
        
class ReportHigherHires(Resource):
    def get(self):
        query="""
                WITH cte as
                (
                SELECT
                    deps.id,
                    deps.department,
                    extract(year from emp.datetime) y,
                    count(*) cnt
                    
                FROM `thinkqwant.globant_tech_test.hired_employees` emp
                JOIN `thinkqwant.globant_tech_test.departments` deps on emp.department_id=deps.id
                JOIN `thinkqwant.globant_tech_test.jobs` jobs on emp.job_id=jobs.id
                WHERE extract(year from emp.datetime)=2021
                GROUP BY y,deps.id,deps.department
                )

                SELECT id,department,cnt hired
                from cte
                WHERE cnt > (select avg(cnt) from cte)
                ORDER BY hired desc
            """
        try:
            client = bigquery.Client()
            bigquery_job = client.query(query)
            df=bigquery_job.result().to_dataframe()
            df = df.reset_index(drop=True)
            response=df.to_json()
            return response
        except:
            return {'Status':'Processing ERROR'}, 500
    
api.add_resource(UpdateDepartments, '/update_departments')
api.add_resource(UpdateHiredEmployees, '/update_hired_employees') 
api.add_resource(UpdateJobs, '/update_jobs') 
api.add_resource(ReportEmployeesHired,'/report_hired_employees')
api.add_resource(ReportHigherHires,'/report_higher_hires')

if __name__ == '__main__':
    app.run()