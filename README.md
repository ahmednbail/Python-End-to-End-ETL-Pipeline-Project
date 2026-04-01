# Python-End-to-End-ETL-Pipeline-Project

DataCraft Academy — ETL Pipeline Project: students build a complete Extract, Transform, Load pipeline using Python. They ingest data from APIs, databases, and CSV files, clean and transform it, load it into a Data Warehouse using a Star Schema, and generate BI reports and insights — simulating a real-world data engineering workflow.

---

## 🏪 Business case: Bike Store Sales Data Warehouse

### 🎯 Business context

A multi-store bicycle retail chain operates across multiple US states. Data comes from:

| Source | Data |
|--------|------|
| **PostgreSQL** (transactional) | Orders |
| **Data lake** | Customers, products, staff, stocks |
| **Live API** | Currency exchange rates |

The goal is a data warehouse that unifies these sources so analytics can answer operational and commercial questions consistently.

### 📊 Key business questions

| Business area | Questions |
|---------------|-----------|
| **Sales** | What is total revenue per store, per month, and per category? |
| **Products** | Which brands and categories sell the most? |
| **Customers** | Who are the top customers by revenue? Which cities and states buy the most? |
| **Staff** | Which staff member generates the most sales? |
| **Inventory** | Which stores are running low on stock? |
| **Currency** | What is revenue in local currencies (USD → AED, EUR, etc.)? |

These questions drive fact and dimension design, grain choices, and reporting in the star schema and downstream BI layer.
