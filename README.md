# Coding Test

This repo contains code for interview based on Data Engineering Code Challenge.

## Table of Contents

- [Data Source](#data_source)
- [Data pipeline design](#data_pipeline_design)

## Data Source <a id="data_source"></a>

Data source for this exercise are csv files found in <strong>files</strong> folder:
<li>departments</li>
<li>hired_employees</li>
<li>jobs</li>

## Data Pipeline Design <a id="data_pipeline_design"></a>

Files mentioned above are located in one AWS S3 Bucket. Transformation required in section 1 occurs with flask's endpoints to export these csv files to Google Bigquery which is data sink for this case.

Also there are two endpoints for reading data from Bigquery tables with required layout in section 2 of coding exercise. These are <strong>report_hired_employees</strong> and <strong>report_higher_hires</strong>.

<img src="img/coding_test.jpg">
