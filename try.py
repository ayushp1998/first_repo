
#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import os
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source
import re
import requests
from airbyte_protocol.models import AirbyteStateMessage, AirbyteStateType, AirbyteStreamState, StreamDescriptor
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
import logging
logger = logging.getLogger()
logging.basicConfig(level=logging.NOTSET)

chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = os.getenv("CHROME_BIN")
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-browser-side-navigation')
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument('--disable-extensions')

driver = webdriver.Chrome(executable_path=os.getenv("CHROME_DRIVER_PATH"), options=chrome_options)
job_type_keys = {
    "full_time": "F",
    "part_time": "P"
}
past_time_keys = {
    "second": "r60",
    "day": "r86400",
    "week": "r604800",
    "month": "r2592000"
}
job_level_keys = {
    "internship": 1,
    "entry_level": 2,
    "associate": 3,
    "mid_senior_level": 4,
    "director": 5
}


class SourceLinkedinJobScrapper(Source):
    driver = None
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:

        from .streams_schema import stream_schema

        return AirbyteCatalog(streams=stream_schema)

    @staticmethod
    def create_soup(url):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup

    def infinite_scroll(self, url, scroll_times, button_class_name, driver_required=True):
        if driver_required:
            self.driver.get(url)
        time.sleep(2)
        scroll_times = scroll_times
        for i in range(scroll_times):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            try:
                if driver_required:
                    button = WebDriverWait(self.driver, 0).until(
                        EC.presence_of_element_located((By.CLASS_NAME, button_class_name)))
                    button.click()
                else:
                    button = WebDriverWait(self.driver, 0).until(EC.element_to_be_clickable((By.CLASS_NAME, button_class_name)))
                    button.click()
            except:
                pass
            time.sleep(1)

        html = self.driver.page_source
        if driver_required:
            self.driver.close()
        return html

    def get_scroll_time_count(self, url, tag_name, class_name, denominator):
        try:
            soup = self.create_soup(url)
            tag = soup.find('span', class_='results-context-header__job-count')
            # print("Tag New", soup.find_all('span'))
            print("tag", tag, url)
            results_text = tag.text
            numeric_part = re.findall(r'\d+', results_text)[0]
            if int(numeric_part) == 1:
                tag2 = soup.find(tag_name, class_='results-context-header__new-jobs')
                results_text2 = tag2.text
                results_text2 = results_text2.replace(',', '')
                numeric_part2 = re.findall(r'\d+', results_text2)[0]
                return int(int(numeric_part2) / denominator)
            return int(int(numeric_part) / denominator)
        except:
            return 1

    def get_all_jobs_jd_links(self, job_role="Software%20Engineer", location="India", job_type="full_time", past_time="day",
                              job_level="entry_level"):
        jd_links = set()
        job_search_base_url = "https://www.linkedin.com/jobs/search?"
        final_url = job_search_base_url
        payload = {
            "keywords": job_role,
            "location": location,
            "f_JT": job_type_keys[job_type],
            "f_TPR": past_time_keys[past_time],
            "f_E": job_level_keys[job_level]
        }

        for key, val in payload.items():
            final_url += f"{key}={val}&"
        scroll_times = self.get_scroll_time_count(final_url, tag_name='span', class_name="results-context-header__job-count",
                                                  denominator=25)
        html = self.infinite_scroll(final_url, scroll_times, button_class_name="infinite-scroller__show-more-button--visible")

        soup = BeautifulSoup(html, 'html.parser')
        ul_tag = soup.find("ul", class_="jobs-search__results-list")
        li_tags = ul_tag.find_all("li")

        for li in li_tags:
            a_tags = li.find_all("a")
            for a in a_tags:
                if "base-card__full-link" in a.get("class", []):
                    jd_links.add(a['href'].split("?")[0])
        return jd_links

    @staticmethod
    def create_open_ai_query(input_query, OPENAI_API_KEY, model_engine='gpt-3.5-turbo', temperature=0):
        openai_url = f"https://api.openai.com/v1/chat/completions"
        headers = {'Authorization': f'Bearer {OPENAI_API_KEY}', 'Content-Type': 'application/json'}
        payload = {
            'model': model_engine,
            'messages': [{"role": "user", "content": input_query}],
            'temperature': temperature,
            'max_tokens': 150
        }
        response = requests.post(openai_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200 and 'choices' in response.json():
            content_text = response.json()['choices'][0]['message']['content'].strip()
            return {"success": True, "data": content_text, "response_json": response.json()}
        return {"success": False, "error": response.text}

    @staticmethod
    def validate_and_send_correct_evaluation_response(text_response):
        text_response = str(text_response).replace('\n', '')
        text_response = str(text_response).replace('null', '')
        evaluation_json = eval(text_response)
        return evaluation_json

    def extract_additional_details_from_job_text(self, jd_text, open_ai_key, model_engine='gpt-3.5-turbo'):
        prompt = "extract these details from the following text and just provide a JSON in this format" \
                 "{'skills': {'preferredSkills': []}, 'min_ctc': , 'max_ctc': , 'min_experience': , 'max_experience': , 'hr_name': '', 'department': }" \
                 "put maximum 5 items inside preferredSkills and those skills should be keywords only." \
                 "if min_ctc, max_ctc, min_experience, max_experience, not available then put it zero" \
                 "hr_name is about any name email or contact number available in the text, put empty string if not there"\
                 "treat this text as job description and extract what portion or department of the company this job description would be for, put that inside department"\
                 f"text: {jd_text}"
        resp = self.create_open_ai_query(prompt, open_ai_key, model_engine=model_engine)
        if resp['success']:
            final_resp = self.validate_and_send_correct_evaluation_response(resp['data'])
            if final_resp['max_experience'] < final_resp['min_experience']:
                final_resp['max_experience'] = final_resp['min_experience']
            return final_resp
        return {}

    @staticmethod
    def get_hiring_team(jd_soup):
        h3 = jd_soup.find('h3', {'class': 'base-main-card__title font-sans text-[18px] font-bold text-color-text overflow-hidden'})
        if h3:
            return h3.text.strip()
        return ''

    @staticmethod
    def remove_html_tags(text):
        clean = re.compile('<.*?>')
        return re.sub(clean, ' ', text)

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:

        config_role = config['job_role']
        if config_role == 'dummy':
            job_roles = ["Angular Developer", "Angular JS Developer", "Associate Software Engineer", "Backend Developer", "C# Developer",
                         "C++ Developer", "Developer", "Client-Side Developer", "Embedded Software Developer", "Embedded Software Engineer",
                         "Front End Web Developer", "Front-End Developer", "Frontend Angular Developer", "Frontend Architect",
                         "Frontend Developer", "Frontend Engineer", "Frontend Web Developer", "Full Stack Developer",
                         "Full Stack Java Developer", "Full Stack Software Engineer", "HTML Developer", "Java Backend Developer",
                         "Java Developer", "Java Fullstack Developer", "Java Microservices Developer", "Java React Developer",
                         "Java SpringBoot Developer", "Javascript Developer", "Junior Software Developer", "Junior Software Engineer",
                         "Mean Stack Developer", "MERN Stack Developer", "MIS", "MIS Analyst", "MIS Executive and Analyst",
                         "Node JS Developer", "Node.js Developer", "Python Developer", "Python/Django Developer", "React Developer",
                         "React Js Developer", "React.js Developer", "React/Frontend Developer", "React+Node Js Developer",
                         "RIM Support Engineer", "Ruby on Rails Developer", "SAP HANA DB Administration Software Development Engineer",
                         "Software Developer", "Software Development Engineer", "Software Engineer", "Software Engineer Trainee",
                         "Software Programmer", "Solution Developer", "SYBASE Database Administration Software Development Engineer",
                         "Trainee Associate Engineer", "Trainee Software Developer", "Trainee Software Engineer", "UI Angular Developer",
                         "UI Developer", "UI Frontend Developer", "UI/Frontend Developer", "UI/UX Developer", "Web and Software Developer",
                         "Web Designer & Developer", "Web Designer and Developer", "Web Designer/Developer", "Web Developer",
                         "Web Developer and Designer", "Website Designer", "website developer", "XML and C# Developer", "PHP Developer",
                         "Laravel Developer", "Magento Developer", "Drupal Developer", "Dotnet developer", ".net ", "Vue.JS Developer",
                         "Python/Django Developer", "GoLang developer", "jQuery", "Springboot Developer", "Actuarial Analyst", "Analyst",
                         "AR Analyst", "Associate Business Analyst", "Automation Test Analyst", "Azure Data Engineer", "Big Data Engineer",
                         "Business Analyst", "Business Data Analyst", "Data Analyst", "Data Analytics Trainer", "Data Research Analyst",
                         "Data Researcher", "Data Science Engineer", "Data Scientist", "Database Administrator", "Functional Analyst",
                         "Junior Analyst", "Junior Research Analyst", "KYC Analyst", "Market Research Analyst", "Power BI Developer",
                         "Product Analyst", "Programmer Analyst", "QA Analyst", "Quality Analyst", "Real Time Analyst",
                         "Reconciliation Analyst", "Research Analyst", "Risk Analyst", "Sales Analyst", "Salesforce Business Analyst",
                         "Service Desk Analyst", "SOC Analyst", "SQL Developer", "Android Application Developer", "Android Developer",
                         "Android Mobile Application Developer", "Application Developer", "Application Support Engineer",
                         "Flutter Developer", "iOS Application Developer", "IOS Developer", "Mobile App Developer",
                         "Mobile Application Developer", "Associate Technical Support Engineer", "Automation Engineer",
                         "Automation Test Engineer", "Batch Support Engineer", "Desktop Support Engineer", "Genesys Support Engineer",
                         "IT Support Engineer", "Network Support Engineer", "QA Automation Engineer", "SaaS Support Engineer",
                         "Security Engineer", "Test Automation Engineer", "Systems Support Engineer",
                         "Software Development Engineer - Test", "Software Test Engineer", "Software Tester", "Support Engineer",
                         "Tech Customer Support Engineer", "Technical Support Engineer", "Servicenow Developer", "SharePoint Developer",
                         "Shopify Developer", "Unity Game Developer", "WordPress & Shopify Developer", "WordPress Developer",
                         "Wordpress Web Developer", "Unreal Developer"]
            for id, jr in enumerate(job_roles):
                job_roles[id] = jr.replace(" ", "%20")
        elif config_role == 'Analyst':
            job_roles = [
                "Analyst",
                "Analytical",
                "analytical skill",
                "Analytical Skills",
                "Analytics",
                "BeautifulSoup",
                "big data",
                "Bigcommerce",
                "BIGDATA",
                "Business analysis",
                "Business process",
                "Business Process Management",
                "Business Requirement Analysis",
                "Caffe",
                "cassandra",
                "Consulting",
                "CuDNN",
                "data acquisition",
                "Data analysis",
                "Data Architecture",
                "Data Engineer",
                "Data Engineering",
                "Data Factory",
                "data governance",
                "Data Loader",
                "Data Management",
                "Data Migration",
                "Data modeling",
                "Data Models",
                "data pipeline architecture",
                "Data processing",
                "data protection",
                "Data quality",
                "data science",
                "Data validation",
                "Data verse in PowerAERROR!",
                "data warehouse",
                "Data-Binding",
                "Database Design",
                "Database management",
                "Database Schema",
                "DAX queries",
                "Db2",
                "Dynamo Db",
                "ETL",
                "ETL design",
                "Excel",
                "Hadoop",
                "IT Security Analyst",
                "Lambda/function",
                "mangodb",
                "microsoft",
                "Microsoft Azure",
                "Microsoft azure data factory",
                "Mongo DB",
                "MongoDB",
                "MS Access",
                "MS Office",
                "MS SQL",
                "Ms Sql Database",
                "Ms Sql Serve",
                "MSMQ",
                "MSSQL",
                "Mysq",
                "MySQL",
                "MySQL. HTML",
                "NLP",
                "NoSQL",
                "OpenCV",
                "Phyton",
                "Pinecone DB",
                "PL/SQL",
                "PLSQL",
                "Postgres",
                "Postgresql",
                "Power BI",
                "Problem Solving",
                "Problem Solving & Analytical Skills",
                "Process Analytics",
                "PySpark",
                "Python",
                "Python Development",
                "Python Framework",
                "python progaraming",
                "RDBMS",
                "Rdbms Concepts",
                "RDS",
                "redshift",
                "Relational database",
                "relational databases",
                "Scala",
                "scrapy",
                "scrapy framework",
                "Spark",
                "SQL",
                "SQL Azure",
                "SQL Database",
                "sql knowledge",
                "SQL queries",
                "SQL Server",
                "SQL Server ASP.Net",
                "SQL Server Development",
                "SQLit",
                "SQLite",
                "SQLite Database",
                "sqs",
                "SSIS",
                "SSRS",
                "Stored procedures",
                "tableau",
                "TensorFlow",
                "Theano",
                "Torch",
                "Triggers",
                "Advanced Excel",
                "BA",
                "business Analyst",
                "Business Analytics",
                "Business Intelligence (BI)",
                "data analyst",
                "data analytics",
                "data cleansing",
                "Data Scraping",
                "Database",
                "Database Planning",
                "database structures",
                "Databases Postgres",
                "ETL Tool",
                "Extraction",
                "Fabrication",
                "Google Analytics",
                "H look up",
                "macros",
                "Management Information System",
                "Microsoft applications",
                "MIS",
                "MS SQLServer",
                "MS-Excel",
                "Nosql Databases",
                "numpy",
                "Outlook",
                "panda",
                "PowerPoint.",
                "Qlik",
                "query",
                "Regression testing",
                "Regular Expressions",
                "spreadsheets",
                "statistical analyses",
                "vlook up",
                "Warehousing",
                "WCF Data Services",
                "Word",
                "AI",
                "analysis",
                "Analysts",
                "Associate Analyst",
                "bi",
                "Bi Tools",
                "BigQuery",
                "business analyst bpo",
                "business intelligence",
                "Business operations",
                "business process analysis",
                "business requirements",
                "Business Research",
                "Business services",
                "business system",
                "Concatenate",
                "CouchD",
                "dashboards",
                "Data collection",
                "data collection systems",
                "Data communication",
                "Data entry operation",
                "data integrity",
                "Data Mapping",
                "data mining",
                "Data Reporting",
                "Data Sciences",
                "data visualization",
                "Data warehousing",
                "Database Management System",
                "Database testing",
                "DB",
                "dbms",
                "deep learning",
                "excel google analytics",
                "Google Sheets",
                "hlookup",
                "Index Optimization",
                "IT Business Analyst",
                "IT Consulting",
                "IT Management",
                "IT Operations Management",
                "looker",
                "Mango Db",
                "Marketing analytics",
                "Marketing operations",
                "Mathematics",
                "Memcached",
                "Microstrategy",
                "MIS documentation",
                "Mis Report Preparation",
                "MIS reporting",
                "Ms excel",
                "Natural language processing",
                "Php And Mysql",
                "Php Codeigniter",
                "Pivot",
                "pivot table",
                "PL-SQL",
                "Portfolio management",
                "postgrest",
                "Power Query",
                "Powerpoint",
                "predictive analytics",
                "prescriptive analytics",
                "Python or PHP",
                "R",
                "Reporting tools",
                "Risk analysis",
                "SAS",
                "Schema",
                "Senior Analyst",
                "Site Analysis",
                "Snowflake DB",
                "SPSS",
                "Statistical process control",
                "Statistical Tools",
                "statistics",
                "System analysis",
                "Systems Analysis",
                "T-SQL",
                "Teradata",
                "VB SCRIPT",
                "VBA",
                "vlookup",
                "Webmaster",
                "Website Analysis",
                "zoho analytics",
            ]
        else:
            job_roles = [config_role]

        for job_role in job_roles:
            self.driver = webdriver.Chrome(executable_path=os.getenv("CHROME_DRIVER_PATH"), options=chrome_options)

            job_role_data = {'title': job_role}
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(stream='job_roles', data=job_role_data, emitted_at=int(datetime.now().timestamp()) * 1000),
            )
            try:
                jd_links = self.get_all_jobs_jd_links(job_role=job_role)
            except Exception as e:
                logger.info("failed jd link", job_role, str(e))
                jd_links = []
            for jd_link in jd_links:
                job_details = {
                    'job_description_url': jd_link,
                    'job_description_url_without_job_id': jd_link,
                    'job_role': job_role,
                    'job_source': 'linkedin',
                    'job_type': 'full-time'
                }
                try:
                    jd_link = jd_link.replace("https://in.", "https://www.")
                    soup = self.create_soup(jd_link)
                    company_details = {}

                    hr_name = self.get_hiring_team(soup)

                    h1_tag = soup.find("h1", class_="top-card-layout__title")
                    if h1_tag:
                        job_details['job_title'] = h1_tag.text

                    company_span_tags = soup.find_all("span", class_=lambda x: x and "topcard__flavor" in x.split())
                    for span in company_span_tags:
                        a_tag = span.find("a")
                        if a_tag:
                            text = a_tag.text.strip()
                            href = a_tag.get("href")
                            company_details['name'] = text
                            company_details['linkedin_url'] = str(href).split("?")[0]

                    details_span_tags = soup.find_all("span", class_=lambda x: x and "topcard__flavor--bullet" in x.split())
                    span_texts = []
                    for span in details_span_tags:
                        span_texts.append(span.text.strip())
                    if len(span_texts) == 1:
                        job_details['job_location'] = span_texts[0]
                    if len(span_texts) == 2:
                        job_details['job_location'] = span_texts[0]
                        job_details['raw_response'] = {'applicants': span_texts[1]}

                    div_tags = soup.find_all("div", class_=lambda x: x and "show-more-less-html__markup" in x.split())
                    for div in div_tags:
                        html_content = div.decode_contents()
                        job_details['job_description_raw_text'] = self.remove_html_tags(html_content.strip())

                    # ul_tag = soup.find("ul", class_=lambda x: x and "description__job-criteria-list" in x.split())
                    # li_tags = ul_tag.find_all("li")
                    # for li in li_tags:
                    #     h3_tag = li.find("h3")
                    #     h3_text = h3_tag.get_text(strip=True) if h3_tag else ""
                    #     span_tag = li.find("span")
                    #     span_text = span_tag.get_text(strip=True) if span_tag else ""
                    #     job_details[h3_text] = span_text

                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(stream='companies', data=company_details, emitted_at=int(datetime.now().timestamp()) * 1000),
                    )

                    job_details['company'] = company_details['name']

                    ai_response = self.extract_additional_details_from_job_text(job_details['job_description_raw_text'],
                                                                                config['open_ai_api_key'])
                    if 'skills' not in ai_response:
                        logger.info('skills not found')
                        ai_response = self.extract_additional_details_from_job_text(job_details['job_description_raw_text'],
                                                                                    config['open_ai_api_key'], model_engine='gpt-4')
                    job_details = job_details | ai_response
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(stream='job_openings', data=job_details, emitted_at=int(datetime.now().timestamp()) * 1000),
                    )
                    recruiter_details = {
                        'short_intro': ai_response['hr_name']
                    }

                    if hr_name:
                        recruiter_details = recruiter_details | {
                            'name': hr_name,
                            'hiring_manager_for_job_link': jd_link,
                            'company': company_details['name'],
                            'linkedin_profile_url': f"dummy_{hr_name}_{company_details['name']}"
                        }

                        yield AirbyteMessage(
                            type=Type.RECORD,
                            record=AirbyteRecordMessage(stream='recruiter_details', data=recruiter_details, emitted_at=int(datetime.now().timestamp()) * 1000),
                        )
                except Exception as e:
                    logger.info("failed", jd_link, str(e))
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(stream='job_openings', data=job_details, emitted_at=int(datetime.now().timestamp()) * 1000),
                    )

        for stream_name in ["companies", "job_openings", "recruiter_details", "job_roles"]:
            yield AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(
                            name=stream_name
                        )
                    ),
                )
            )
