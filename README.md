# DATA-PIPELINE-DEVELOPMENT

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: PULI NARMADA DEVI

*INTERN ID*:CT06DH143

*DOMAIN*: DATA SCIENCE

*DURATION*: 6 WEEKS

*MENTOR*: NEELA SANTOSH

**MY FIRST TASK IS TO CREATE A PIPELINE FOR DATA PREPROCESSING, TRANSFORMATION, AND LOADING WITH AUTOMATION**
The setup process began with the installation of Airflow itself, along with several critical extensions to enhance functionality and usability. With Airflow up and running, my first coding steps involved importing essential Python libraries that are foundational to data manipulation and ETL (Extract, Transform, Load) processes. The libraries I included in my project were Pandas for data handling, NumPy for numerical operations, and Schedule for task scheduling, among others. This diverse toolkit laid the groundwork for a robust automation pipeline.

The core function of the data pipeline is to manage data efficiently, and I started by focusing on the extraction phase. For this, I specifically utilized a CSV file as my source of data. With Pandas, I created a DataFrame to read the contents of the CSV, thus facilitating data manipulation. This initial extraction was crucial as it set the stage for further transformation.

Once the data was extracted, I moved on to the transformation stage of the ETL process. Here, I implemented a variety of transformations such as data cleaning, filtering out unnecessary entries, and performing calculations on specific fields. This is where I leveraged the power of Pandas to ensure that the data was not only accurate but also relevant for the intended analysis.

After thorough transformations were applied and the data was tailored to meet our needs, the next step was to load the transformed data into a specified database table. For this task, I chose SQLAlchemy, a powerful SQL toolkit and Object-Relational Mapping (ORM) system for Python. I configured the connection parameters and created a URL that pointed to my database, ensuring the data could be stored and accessed efficiently.

With the ETL pipeline poised for execution, I proceeded to run it. This involved taking the cleaned and transformed data, pushing it to the database, and utilizing the URL I had established as the data source. The entire pipeline was coded to not only extract data directly from the database but also to re-run ETL operations seamlessly, enabling continuous data flow.

Scheduling the ETL pipeline was a pivotal step in ensuring regular updates and data accuracy. I set it to run daily at a specific time, which was integral for maintaining up-to-date datasets. Additionally, I crafted a specific test case to validate the pipeline’s functionality. This example used a dataset retrieved from GitHub, complete with the designated URL and the timing parameters for the daily execution of the pipeline.

To ensure robustness, I incorporated exception handling mechanisms within the functions. This feature allowed the system to manage errors gracefully, thereby enhancing reliability. It’s important to note that I included optional verification steps for loaded data, primarily aimed at debugging and testing purposes. However, these validation processes would only execute if the script wasn't perpetually running for scheduling. If needed, I could stop the script and run this section separately to check for data integrity.

Through this comprehensive process, I not only honed my skills in coding and automation but also gained a deeper understanding of how data pipelines function. Each aspect of the pipeline—from extraction and transformation to loading and scheduling—provided invaluable insight into the data management landscape.

**output**
1.<img width="720" height="248" alt="Image" src="https://github.com/user-attachments/assets/979bfbf6-da54-4314-bf61-ba504d14511a" />
2.<img width="773" height="260" alt="Image" src="https://github.com/user-attachments/assets/cf6adb04-edaf-45a3-9735-7eaab531a320" />
