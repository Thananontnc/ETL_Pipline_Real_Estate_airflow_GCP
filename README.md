# ETL_Pipline_Real_ESTATE

I use dataset from Kaggle 
Here is the dataset: https://www.kaggle.com/datasets/ahmedshahriarsakib/usa-real-estate-dataset/code

## This is architecture of this project
![RealestateArchitecture](https://github.com/user-attachments/assets/f2a8a894-22f1-492f-83c2-897207544b52)

The project aims to perform the following tasks:

1. Data Extraction: Extract data using python.
2. Data Transformation : i trasform the data and decoding data into the easy reading form.
3. Data Loading: Load transformed data into Google BigQuery tables.
4. Orchestration: Automate complete Data pipeline using Airflow ( Cloud Composer )
5. Do Data Analysis in Google Looker Studio

### Overall of analysis:
![Screen Shot 2567-11-27 at 23 05 29 PM](https://github.com/user-attachments/assets/7c34f0f2-0da4-4bc4-9354-ff8ad8e38e14)

1. Average Price in Each City
Observation:
* The first graph shows the average property price in various cities.
* Cities such as Brooklyn, New York, and Miami have significantly higher average property prices compared to others.
* Notably, Brooklyn has the highest average price exceeding $3M, while smaller cities like Atlanta and Baltimore have much lower averages.
Insight:
* High prices in Brooklyn and New York indicate these are prime real estate markets with high demand.
* Cities with lower average prices may present opportunities for budget-friendly investments.

2. Average Number of Bedrooms and Bathrooms
Observation:
* The second graph compares the average number of bedrooms and bathrooms in different cities.
* Most cities have an average of 3 bedrooms and 2-3 bathrooms.
* Naples has slightly more bedrooms and bathrooms compared to others, suggesting larger homes on average.
Insight:
* Cities with higher bedroom and bathroom counts, like Naples, might cater to families or buyers seeking spacious homes.
* This insight can guide buyers prioritizing space or investors targeting family-oriented markets.

3. Relationship Between House Size and Price Per Square Foot
Observation:
* The bubble chart illustrates the relationship between house size (x-axis) and price per square foot (y-axis).
* Smaller homes tend to have higher price-per-square-foot values, while larger homes generally have lower price-per-square-foot values.
* Cities such as Rockholds and Pierson show extremes, where smaller homes demand a high price per square foot.
Insight:
* High price-per-square-foot values for smaller homes suggest demand for compact properties in specific locations.
* Larger homes in cities like Culebra and Palenville may offer better value for money when analyzed on a per-square-foot basis.

Conclusion
* Brooklyn and New York are premium markets for high-value properties, while cities like Atlanta and Baltimore may suit investors looking for affordability.
* Cities like Naples offer larger homes, likely targeting family buyers.
* Smaller properties in cities like Rockholds and Pierson demand premium prices, while larger homes in other areas provide better cost efficiency.
