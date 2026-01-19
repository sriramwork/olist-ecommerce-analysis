# olist-ecommerce-analysis
End-to-end SQL analysis of Brazilian e-commerce data involving ETL, RFM segmentation, and Cohort Analysis.

# Olist Brazil E-Commerce Business Intelligence & RFM Analysis

### 📊 Project Overview
This project transforms over 100,000 raw customer records from the Olist Brazil E-Commerce dataset into a structured analytical engine. I built an end-to-end SQL pipeline to solve strategic business problems in logistics, marketing, and customer retention.

### 🔗 Data Source
The dataset used for this analysis is the **Brazilian E-Commerce Public Dataset by Olist**, hosted on Kaggle.
* **Dataset Link:** [Kaggle - Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### 🛠️ Project Architecture
The repository follows a professional data warehousing approach:
1. **Data Engineering:** Built a modular ETL pipeline moving from a `raw` staging schema to an `analytics` layer.
2. **Descriptive Analytics:** Developed KPIs for monthly revenue trends and logistics performance.
3. **CRM Analytics (RFM):** Engineered a segmentation model to cluster customers into groups like "Champions" and "At-Risk."
4. **Retention Analytics:** Designed monthly cohort retention heatmaps to analyze platform "stickiness."

### 📈 Key Business Insights
* **Retention Reality:** Cohort analysis revealed that Olist is primarily an acquisition-heavy business, with most customer cohorts showing sharp drop-offs after the first month.
* **Black Friday Impact:** Identified a 53% revenue spike in November 2017.
* **Logistics Efficiency:** Calculated an average delivery time of 12.5 days with an 11.5% late delivery rate.

### ⚙️ Technical Skills Demonstrated
* **Advanced SQL:** Window Functions (`NTILE`, `RANK`), `COALESCE`, and `PERIOD_DIFF`.
* **Data Modeling:** Schema design, ETL, and Data Pivoting (`MAX(CASE...)`).
