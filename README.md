# BDM – Project 2: Big Data Management Backbone

This project was developed for the **Big Data Management (BDM)** course at FIB – UPC.  
It implements a **data management backbone** to perform descriptive and predictive analysis of housing data in Barcelona and its relationship with socioeconomic indicators.

---

## Overview

The project focuses on building a data pipeline that integrates, cleans, and reconciles multiple datasets into a **Formatted Zone** and then enables analysis in an **Exploitation Zone**.

- **Descriptive analysis**: KPIs to summarize housing and income data  
  - Average density per neighbourhood
  - Average income per neighbourhood
  - Average price by area per neighbourhood
  - Average rent per neighbourhood
    
- **Predictive analysis**: Machine learning models to estimate or classify key outcomes  
  - Predict rental price for a new apartment  
  - Evaluate prediction errors relative to real prices  

---

## Datasets

The backbone integrates several data sources:

- **Mandatory datasets**:  
  - Barcelona rentals (Idealista)  
  - Territorial distribution of income (Open Data Barcelona)  
  - Lookup tables for neighborhoods and districts  

- **Optional extra dataset**: From Open Data BCN: *Demographic indicators. Population density (inhabitants / ha) of the city of Barcelona*

---

## Methods & Technologies

- **Data Integration & Cleaning**  
  - Handling duplicates and reconciliation across multiple sources  
  - Storage in MongoDB collections

- **Computation & Analysis**  
  - Apache Spark for integration, reconciliation, KPI calculation, and machine learning  
  - Spark MLlib for training and validating predictive models  

- **Visualization**  
  - KPIs and streaming results displayed via a Tableau dashboard.  

---

## Objectives Achieved

1. Integrated datasets into the **Formatted Zone**, ensuring consistency and removing duplicates.  
2. Computed and stored four KPIs in the **Exploitation Zone**.  
3. Prepared and trained a machine learning model (Spark MLlib).  
4. Produced graphical visualizations of the analyses.  

---

## Keywords

- Big Data Management  
- Apache Spark · Spark MLlib 
- Data Pipelines  
- Predictive Analytics  
- Barcelona Housing & Economy  
