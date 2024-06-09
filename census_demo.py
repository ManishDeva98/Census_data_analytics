import pandas as pd
from pymongo.mongo_client import MongoClient
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData
import mysql.connector
from tabulate import tabulate

census_raw = pd.read_csv("D:\\python workbook\\census_2011.csv")


####_____________________________________STEP 1__________________________________________________####
#to rename columns
def step1():
    census_raw.rename(columns={
        "State name": "State_or_UT",
        "District name": "District",
        "Male_Literate": "Literate_Male",
        "Female_Literate": "Literate_Female",
        "Rural_Households": "Households_Rural",
        "Urban_Households": "Households_Urban",
        "Age_Group_0_29": "Young_and_Adult",
        "Age_Group_30_49": "Middle_Aged",
        "Age_Group_50": "Senior_Citizen",
        "Age not stated": "Age_Not_Stated"
    }, inplace=True)
    print("columns got renamed- step 1 completed")

####_____________________________________STEP 2__________________________________________________####
#function to normalize the state_names in Camel words
def normalize_state_ut_Camel(state_name):
    words = state_name.lower().split()
    for i, word in enumerate(words):
        if word != "and" or i == 0:
            words[i] = word.capitalize()
    return " ".join(words)

#applying normalize_state_ut_Camel function to all the rows of column 'State_or_UT'
def step2():
    census_raw["State_or_UT"] = census_raw["State_or_UT"].apply(normalize_state_ut_Camel)
    print("updated State/UT vlaues to Camel Case - step2 completed")


####_____________________________________STEP 3__________________________________________________####
#updating state as Telangana if district presents in the 'Telangana.txt' file

def step3():
    telangana_districts=open("Telangana.txt", "r")
    telangana_districts_list= telangana_districts.read().splitlines()
    #print(telangana_districts_list)
    ladakh_district_list=['Jammu and Kashmir']
    #updating states
    census_raw.loc[census_raw['District'].isin(telangana_districts_list), 'State_or_UT'] = 'Telangana'
    census_raw.loc[census_raw['District'].isin(ladakh_district_list), 'State_or_UT'] = 'Ladakh'
    print("updated State based on new district(Telungana/Ladakh)-step 3 completed")

####_____________________________________STEP 4__________________________________________________####
#updating the missing values

def step4():
    missing_value=(census_raw.isnull().sum())
    print("missing values before processing missing data")
    print(missing_value)

    census_raw['Population'].fillna(census_raw['Male']+census_raw['Female'],inplace=True)
    census_raw['Male'].fillna(census_raw['Population']-census_raw['Female'],inplace=True)
    census_raw['Female'].fillna(census_raw['Population']-census_raw['Male'],inplace=True)
    census_raw['Literate'] .fillna( census_raw['Literate_Male'] + census_raw['Literate_Female'],inplace=True)
    census_raw['Literate_Male'].fillna(census_raw['Literate']-census_raw['Literate_Female'],inplace=True)
    census_raw['Literate_Female'].fillna(census_raw['Literate']-census_raw['Literate_Male'],inplace=True)
    census_raw['SC'].fillna(census_raw['Male_SC']+census_raw['Female_SC'],inplace=True)
    census_raw['Male_SC'].fillna(census_raw['SC']-census_raw['Female_SC'],inplace=True)
    census_raw['Female_SC'].fillna(census_raw['SC']-census_raw['Male_SC'],inplace=True)
    census_raw['ST'].fillna(census_raw['Male_ST']+census_raw['Female_ST'],inplace=True)
    census_raw['Male_ST'].fillna(census_raw['ST']-census_raw['Female_ST'],inplace=True)
    census_raw['Female_ST'].fillna(census_raw['ST']-census_raw['Male_SC'],inplace=True)
    census_raw['Workers'].fillna(census_raw['Male_Workers']+census_raw['Female_Workers'],inplace=True)
    census_raw['Male_Workers'].fillna(census_raw['Workers']-census_raw['Female_Workers'],inplace=True)
    census_raw['Female_Workers'].fillna(census_raw['Workers']-census_raw['Male_Workers'],inplace=True)
    census_raw['Total_Education'].fillna(census_raw['Literate_Education']+census_raw['Illiterate_Education'],inplace=True)
    census_raw['Literate_Education'].fillna(census_raw['Total_Education']-census_raw['Illiterate_Education'],inplace=True)
    census_raw['Illiterate_Education'].fillna(census_raw['Total_Education']-census_raw['Literate_Education'],inplace=True)

    missing_value=(census_raw.isnull().sum())
    print("missing values after processing missing data")
    print(missing_value)

    census_raw.to_csv("updated_missing_values_1.csv", index=False)
    
    print("Handelled possible missing datas - step 4 completed ")

####_____________________________________STEP 5__________________________________________________####
#storing the processed data in mongo db

def step5():
    # Load the processed data into a DataFrame
    processed_data = pd.read_csv("updated_missing_values_1.csv")
    # Convert DataFrame to dictionary
    data_dict = processed_data.to_dict(orient='records')

    client = MongoClient("mongodb+srv://manishdeva:manish1234@cluster0.3rnfpb7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  
    db = client['Census_demo'] 
    collection = db['census']

    collection=db.get_collection("census")
    result=collection.delete_many({})
    # Insert data into MongoDB collection
    collection.insert_many(data_dict)
    client.close()

    print("Data saved to MongoDB successfully.")
    print("step 5 completed")
####_____________________________________STEP 6__________________________________________________####
#reading the data from mongo db and loading it into SQL server

def step6():
    # MongoDB connection details
    mongo_client = MongoClient("mongodb+srv://manishdeva:manish1234@cluster0.3rnfpb7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    mongo_db = mongo_client['Census_demo']
    mongo_collection = mongo_db['census']

    # Fetch data from MongoDB
    data = list(mongo_collection.find())


    # Convert MongoDB data to DataFrame
    df = pd.DataFrame(data)
    df.rename(columns = {'Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car'
                        :'Households_with_TV_Computer_Laptop_Telephone_mobile_Scooter_Car',
                        'Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households':
                        'Type_of_latrine_facility_Night_soil_disposed_into_open_drain',
                        'Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households':
                        'Type_of_latrine_facility_pour_flush_joined_to_other_system',
                        'Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households':
                        'Not_having_latrine_facility_within_the_premises_Alter_source',
                        'Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households':
                        'Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Houses',
                        'Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households':
                        'Main_source_of_water_Other_Spring_River_Canal_Tank_Pond_Lake'}, inplace = True)
    print("\nAfter modifying first column:\n", df.columns)

    print(df)
    df['_id'] = df['_id'].astype(str)

    # MySQL connection details
    mysql_engine = create_engine('mysql+mysqlconnector://root:@localhost:3306/census_1')


    # Define table schema
    metadata = MetaData()


    # Create tables in the MySQL database
    metadata.create_all(mysql_engine)

    # Insert data into MySQL
    df.to_sql('census', mysql_engine, if_exists='replace', index=False)

    print("Data has been successfully saved to MySQL.")
    print("step 6 completed")


####_____________________________________STEP 7__________________________________________________####
#connecting to mysql server
def step7():

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    )
    mycursor = mydb.cursor(buffered=True)
    
    mycursor.execute("SELECT district, SUM(population) AS total_population FROM census_1.census GROUP BY district")
    out=mycursor.fetchall()
    print("1. What is the total population of each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("SELECT district,sum(Literate_Male),sum(Literate_Female) FROM census_1.census GROUP BY district")
    out=mycursor.fetchall()
    print("2.How many literate males and females are there in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT district,ROUND(SUM(Male_Workers) * 100.0 / SUM(Workers)) AS male_percentage,
    ROUND(SUM(Female_Workers) * 100.0 / SUM(Workers)) AS female_percentage
    FROM
    census_1.census
    GROUP BY
    district""")
    out=mycursor.fetchall()
    print("3.What is the percentage of workers (both male and female) in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT district,sum(LPG_or_PNG_Households)from census_1.census GROUP BY district""")
    out=mycursor.fetchall()
    print("4.How many households have access to LPG or PNG as a cooking fuel in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT district, 
    SUM(Hindus) as Hindus_count, 
    SUM(Muslims) as Muslims_count, 
    SUM(Christians) as Christians_count, 
    SUM(Jains) as Jains_count, 
    SUM(Sikhs) as Sikhs_count, 
    SUM(Buddhists) as Buddhists_count, 
    SUM(Others_Religions) as Others_Religions_count 
    FROM census_1.census group by District""")
    out=mycursor.fetchall()
    print("5.What is the religious composition (Hindus, Muslims, Christians, etc.) of each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT district, SUM(Households_with_Internet) as SUM_Households_with_Internet FROM census_1.census group by District""")
    out=mycursor.fetchall()
    print("6.How many households have internet access in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT district,
    SUM(Below_Primary_Education) AS Below_Primary_Education_count,
    SUM(Primary_Education) AS Primary_Education_count,
    SUm(Middle_Education) AS Middle_Education_count,
    SUM(Secondary_Education) AS Secondary_Education_count,
    SUM(Higher_Education) AS Higher_Education_count,
    SUM(Graduate_Education) AS Graduate_Education_count,
    SUM(Other_Education) AS Other_Education_count
    FROM census_1.census GROUP BY District""")
    out=mycursor.fetchall()
    print("7.What is the educational attainment distribution (below primary, primary, middle, secondary, etc.) in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT District,
    SUM(Households_with_Bicycle) AS Bicycle_mode_count,
    SUM(Households_with_Car_Jeep_Van) AS Car_Jeep_Van_mode_count,
    SUM(Households_with_Scooter_Motorcycle_Moped) AS Motorcycle_count
    FROM census_1.census
    GROUP BY District""")
    out=mycursor.fetchall()
    print("8.How many households have access to various modes of transportation (bicycle, car, radio, television, etc.) in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT District,
    SUM(Condition_of_occupied_census_houses_Dilapidated_Households) AS Dilapiated_count,
    SUM(Households_with_separate_kitchen_Cooking_inside_house) AS Separate_kitchen_count,
    SUM(Having_bathing_facility_Total_Households) AS Bathing_facility_count,
    SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Latrine_facility_count
    FROM census_1.census
    GROUP BY District""")
    out=mycursor.fetchall()
    print("9.What is the condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, with latrine facility, etc.) in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT District,
    SUM(Household_size_1_person_Households) AS One_person_count,
    SUM(Household_size_2_persons_Households) AS Two_person_count,
    SUM(Household_size_1_to_2_persons) AS One_to_Two_person_count,
    SUM(Household_size_3_persons_Households) AS Three_person_count,
    SUM(Household_size_3_to_5_persons_Households) AS Three_to_Five_person_count,
    SUM(Household_size_4_persons_Households) AS Four_person_count,
    SUM(Household_size_5_persons_Households) AS Five_person_count,
    SUM(Household_size_6_8_persons_Households) AS Six_to_Eight_person_count,
    SUM(Household_size_9_persons_and_above_Households) AS Eight_and_Above_person_count
    FROM census_1.census
    GROUP BY District""")
    out=mycursor.fetchall()
    print("10.How is the household size distributed (1 person, 2 persons, 3-5 persons, etc.) in each district?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT, sum(Households) from census_1.census GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("11.What is the total number of households in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,SUM(Having_latrine_facility_within_the_premises_Total_Households) AS count_Latrine_within_premise from census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("12.How many households have a latrine facility within the premises in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,SUM(Households)/COUNT(Households) AS Average_household_size
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("13.What is the average household size in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,
    UM(Ownership_Owned_Households) AS count_owned_house ,
    SUM(Ownership_Rented_Households) AS count_rented_house
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("14.How many households are owned versus rented in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,
    SUM(Type_of_latrine_facility_Pit_latrine_Households) AS count_Pit_latrine,
    SUM(Type_of_latrine_facility_Other_latrine_Households) AS count_other_letrine,
    SUM(Type_of_latrine_facility_Night_soil_disposed_into_open_drain) AS count_Night_soil_disposed,
    SUM(Type_of_latrine_facility_pour_flush_joined_to_other_system) AS count_pour_flush_joined
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("15.What is the distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,
    SUM(Location_of_drinking_water_source_Near_the_premises_Households) AS count_drinking_water_near_house
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("16.How many households have access to drinking water sources near the premises in each state??")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""select State_or_UT,
    SUM(Power_Parity_Less_than_Rs_45000)/count(Power_Parity_Less_than_Rs_45000) AS Avg_income_less_than_45000,
    SUM(Power_Parity_Rs_45000_90000)/count(Power_Parity_Rs_45000_90000) AS Avg_income_less_than_45000,
    SUM(Power_Parity_Rs_90000_150000)/count(Power_Parity_Rs_90000_150000) AS Avg_income_between_90000_150000,
    SUM(Power_Parity_Rs_45000_150000)/count(Power_Parity_Rs_45000_150000) AS Avg_income_between_45000_150000,
    SUM(Power_Parity_Rs_150000_240000)/count(Power_Parity_Rs_150000_240000) AS Avg_income_between_150000_240000,
    SUM(Power_Parity_Rs_240000_330000)/count(Power_Parity_Rs_240000_330000) AS Avg_income_between_240000_330000,
    SUM(Power_Parity_Rs_150000_330000)/count(Power_Parity_Rs_150000_330000) AS Avg_income_between_150000_330000,
    SUM(Power_Parity_Rs_330000_425000)/count(Power_Parity_Rs_330000_425000) AS Avg_income_between_330000_425000, 
    SUM(Power_Parity_Rs_425000_545000)/count(Power_Parity_Rs_425000_545000) AS Avg_income_between_425000_545000,
    SUM(Power_Parity_Rs_330000_545000)/count(Power_Parity_Rs_330000_545000) AS Avg_income_between_330000_545000,
    SUM(Power_Parity_Above_Rs_545000)/count(Power_Parity_Above_Rs_545000) AS Avg_income_above_545000
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("17.What is the average household income distribution in each state based on the power parity categories?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    
    mycursor.execute("""SELECT State_or_UT,
    Married_couples_1_Households*100/(select sum(Married_couples_1_Households) from census_1.census) as pct_1_couple,
    Married_couples_2_Households*100/(select sum(Married_couples_2_Households) from census_1.census) as pct_2_couple,
    Married_couples_3_Households*100/(select sum(Married_couples_3_Households) from census_1.census) as pct_3_couple,
    Married_couples_3_or_more_Households*100/(select sum(Married_couples_3_or_more_Households) from census_1.census) as pct_3_or_more_couple,
    Married_couples_4_Households*100/(select sum(Married_couples_4_Households) from census_1.census) as pct_4_couple,
    Married_couples_5__Households*100/(select sum(Married_couples_5__Households) from census_1.census) as pct_5_couple
    FROM census_1.census
    GROUP BY State_or_UT""")
    out=mycursor.fetchall()
    print("18.What is the percentage of married couples with different household sizes in each state?")
    print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))
    


step1()
step2()
step3()
step4()
step5()
step6()
step7()