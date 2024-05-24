# Independent Learning Project: Data Engineering and Exploration of NYC Taxi Trips
project status: WIP 🚧

## Introduction
This repo contains my _unguided_ and _open-ended_ data project inspired by [Datacamp Unguided Projects](https://www.datacamp.com/blog/introducing-unguided-projects-the-worlds-first-interactive-code-along-exercises). It which primarily aims further apply and develop my data engineering and analysis skills using real-world dataset. The project uses the [NYC Taxi and Limousine Commission (NYC TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) presented a valuable opportunity due to its size, complexity, and publicly available nature.

I think this kind of project is much better than the guided projects when it comes to improving our skills. Its unguided nature compels us to discover what we know and don't know, something we don't usually experience when doing guided projects. 

That is not to say guided projects have less value! Guided projects are one of the best ways to have hands-on experience and develop foundational knowledge.

## Installation
1. Install `poetry` (or use `pip`, see notes below)
2. Create a virtual environment at the root directory
3. At the root directory, run `poetry install` to install the project dependencies found at `pyproject.toml`
4. Once installed, navigate to `src/de_portfolio_nyc_tlc` and follow the instructions listed on the `README.md` to run dagster
        
> Note: if you prefer using `pip`, just copy the dependencies from `pyproject.toml` at the root directory and adjust it to your pip installation config

## Problem Objectives
This project aims to achieve two key objectives:

**Develop Data Engineering Skills.** I seek to gain hands-on experience in the data engineering lifecycle by working with a real-world dataset. This include practicing data ingestion, transformation, cleaning, and exploration using relevant tools and techniques.

**Conduct Exploratory Data Analysis (EDA) of NYC Taxi Trip Data.** I am leveraging the NYC TLC trip data to explore patterns, trends, and insights within the dataset. This involves understanding the data structure, identifying data quality issues, and performing initial analysis to uncover potential areas for further investigation.

This approach allows me to:
- Further learn and apply data engineering concepts through practical implementation
- Gain familiarity with the NYC TLC data and its potential uses for data-driven decision making (although for hypothetical scenarios for now)
- Develop my overall data skills 🙌🏻

## Data Sources
For this project, I'm using the following sources:
-  [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to download the `.parquet` files prepared by the NYC TLC
-  [NYC Open Data API](https://dev.socrata.com/foundry/data.cityofnewyork.us/qp3b-zxtp) to stream download NYC TLC trip records via `httpx.stream()`

## Technologies Used
* Of course, Python and SQL!!! 🍞🧈 
* Dagster (orchestration)

## Approach
- TBF

## Results/Impact
- TBF
