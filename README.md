# Olist E-Commerce: $2.1M Growth Opportunity Analysis

> **End-to-end analytics project identifying retention, logistics, and segmentation opportunities in Brazilian e-commerce.**

**[📊 View Live Dashboard on Tableau Public →](https://public.tableau.com/views/OlistBrazilEnd-to-EndE-CommerceStrategyCustomerRetentionSuite/OlistBrazilE-CommerceGrowthRetentionAnalysis?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)** 

---

## 📊 Executive Summary

Analyzed **96,478 delivered orders** from Olist (Brazil's leading e-commerce platform) to uncover actionable growth strategies. By bridging the gap between SQL-based customer segmentation and executive strategy, this analysis identifies a **$2.1M total addressable opportunity** in Year 1.

### Key Findings

| Opportunity | Impact | Implementation |
| --- | --- | --- |
| 🔴 **Retention Crisis** | **$1.3M Annually** | Launch Second-Purchase Incentive Program |
| 🟡 **Logistics Trust Gap** | **$308K Annually** | Update Delivery Messaging on Checkout |
| 🟢 **Segment Optimization** | **$500K+ Recurring** | Automated RFM-based Email Campaigns |

---

## 🎯 Business Problem

Olist faced three critical challenges identified through deep-dive analysis:

1. **Retention Cliff:** 97% customer churn after the first purchase (Industry benchmark: 20-30%).
2. **Under-Marketed Efficiency:** Delivering **10-20 days early** on average, but using conservative checkout estimates.
3. **Generic Marketing:** Undifferentiated approach across 93,000+ customers.

---

## 🛠️ Technical Stack & Skills Demonstrated

**SQL & Data Engineering:**

* ✅ **Window Functions:** (`NTILE`, `RANK`, `PERIOD_DIFF`, `DATEDIFF`).
* ✅ **CTEs:** Common Table Expressions for modular, readable queries.
* ✅ **ETL Pipeline:** Datetime parsing (`STR_TO_DATE`) and schema separation.
* ✅ **Dimensional Modeling:** Multi-table `INNER` and `LEFT` joins.

**Business Intelligence & Visualization:**

* ✅ **Tableau Public:** Calculated fields, custom sorting, and interactive parameters.
* ✅ **Analytics Frameworks:** RFM Segmentation, Cohort Analysis, and ROI Prioritization.

---

## 💡 Strategic Recommendations (Priority Matrix)

### Priority 1: Second-Order Incentive (Immediate)

* **Action:** Automated email trigger 7 days post-delivery with 15% discount code.
**ROI Logic:**
- Target: **97k** churned customers × **10%** return rate = **9,700** users
- Revenue: **9,700** × **$160** AOV = **$1.55M** gross
- Discount cost: **$233k** (15% margin impact)
- **Net Impact: $1.3M annually**



### Priority 2: Logistics Trust Update (30 Days)

* **Action:** Update checkout messaging to "7-10 days" in top states (SP, RJ, MG).
* **Risk Mitigation:** Verify **90th percentile** delivery remains < 10 days before updating promises.

### Priority 3: Automated RFM Campaigns (90 Days)

* **Action:** Send "Thank You" perks to **Champions** for retention and "We Miss You" bundles to **At-Risk** customers for **Reactivation**.

---

## 💭 Lessons Learned & Future Work

* **Analytical Humility:** I recognized that using "Average Days Early" can hide regional variance. If I had more time, I would focus on **90th percentile variance** to ensure Priority 2 doesn't damage brand trust.
* **Experimentation:** Future work should include **A/B Testing** the 15% discount code to measure true incrementality.
* **CLV Modeling:** Transitioning from simple RFM to **Customer Lifetime Value (CLV)** projections using Python.

---

## 📂 Repository Structure

```
olist-ecommerce-analysis/
│
├── olist_analysis.sql          # Full SQL script (ETL → Analytics → RFM → Cohorts)
├── README.md                    # This file
└── screenshots/                 # Dashboard images
    ├── executive_overview.png
    ├── cohort_retention.png
    └── priority_matrix.png

```


## 🔗 Data Source

* **Dataset:** Brazilian E-Commerce Public Dataset by Olist.
* **Source:** [Kaggle - Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
* **Scope:** 96,478 delivered orders analyzed (97% of total).


## 📫 Connect With Me

**Manikyala Sriram Theerdh** | Data Analyst  
🎓 University of Arizona (Class of 2026)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/sriram0712)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?style=flat&logo=gmail)](mailto:manikyala@arizona.edu)

💼 Open to Data Analyst, BI, and Analytics roles 
