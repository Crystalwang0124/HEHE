import sqlite3 as sqlite
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def create_table_Bars():
    try:
        conn = sqlite.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Can't find the database!")

    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT, 
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewDate' TEXT NOT NULL,
        'CocoaPercent' REAL NOT NULL,
        'CompanyLocationId' INTEGER NOT NULL,
        'Rating' REAL NOT NULL,
        'BeanType' TEXT NOT NULL,
        'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)

    with open(BARSCSV, encoding='utf-8') as f:
        csvReader = csv.reader(f)
        next(csvReader)
        for row in csvReader:
            statement = 'SELECT Id FROM Countries WHERE EnglishName = "{}"'.format(row[5])
            cur.execute(statement)
            res1 = cur.fetchall()
            #print(res1)
            res1 = res1[0][0]

            try:
                statement = 'SELECT Id FROM Countries WHERE EnglishName = "{}"'.format(row[8])
                #print(row[8])
                cur.execute(statement)
                res2 = cur.fetchall()
                #print(res2)
                res2 = res2[0][0]
            except:
                res2 = "unknow"

            row_4 = float(str(row[4]).strip('%'))
            insertion = (row[0],row[1],row[2],row[3],row_4,res1,row[6],row[7],res2)
            statement = 'INSERT INTO "Bars" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)'
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)

    conn.commit()
    conn.close()

def create_table_Countries():
    try:
        conn = sqlite.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Can't find the database!")

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    statement = '''
    CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT, 
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT NOT NULL,
        'Subregion' TEXT NOT NULL,
        'Population' INTEGER NOT NULL,
        'Area' REAL
        );
    '''
    cur.execute(statement)

    c_file = open(COUNTRIESJSON, encoding='utf-8')
    c_str = c_file.read()
    c = json.loads(c_str)
    for i in c:
        insertion = (i['alpha2Code'], i['alpha3Code'], i['name'], i['region'], i['subregion'], i['population'], i['area'])
        statement = 'INSERT INTO "Countries" (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

    conn.commit()
    conn.close()           

# Part 2: Implement logic to process user commands
def bars_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10"):
    #connect to Database
    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    # select outputs
    if "C1" in specification:
        statement = "SELECT SpecificBeanBarName, Company, C1.EnglishName, Rating, CocoaPercent, C2.EnglishName "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
        statement += "LEFT JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "
    elif "C2" in specification:
        statement = "SELECT SpecificBeanBarName, Company, C1.EnglishName, Rating, CocoaPercent, C2.EnglishName "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
        statement += "LEFT JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "
    else:
        statement = "SELECT SpecificBeanBarName, Company, C1.EnglishName, Rating, CocoaPercent, C2.EnglishName "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
        statement += "LEFT JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "

    # format statement
    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
        try:
            statement += "WHERE {} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify bars command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("Rating")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("CocoaPercent")

    #top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {} ".format(limit)
    #print(statement)
    #return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


def companies_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10"):
    #connect to Database
    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    if criteria == "ratings":
        statement = "SELECT Company, C1.EnglishName, AVG(Rating) "
    elif criteria == "cocoa":
        statement = "SELECT Company, C1.EnglishName, AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement = "SELECT Company, C1.EnglishName,COUNT(SpecificBeanBarName) "

    statement += "FROM Bars JOIN Countries as C1 ON Bars.CompanyLocationId = C1.Id "

    # format statement
    if "C1.Alpha2" in specification:
        statement += "JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif "C2.Alpha2" in specification:
        statement += "JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif specification == "Alpha2" or specification == "Region":
        statement += "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    else:
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
        try:
            statement += "AND Countries.{} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify companies command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {}".format(limit)
    print(statement)
    # return a list of tuples
    results = []
    # print(statement)
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


def countries_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10", sources="sellers"):
    ##connect to Database
    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    statement = "SELECT EnglishName, Region, "

    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    if sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "

    statement += "GROUP BY EnglishName "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    if specification != "":
        if "Region" in specification:
            keyword = keyword.title()
        try:
            statement += "AND {} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify countries command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit result number
    statement += "LIMIT {} ".format(limit)

    # return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results



def regions_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10", sources="sellers"):
    #connect to Database
    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    statement = "SELECT Region, "

    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    if sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "

    statement += "GROUP BY Region "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit result number
    statement += "LIMIT {} ".format(limit)

    # return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


# formate outputs
def str_output(str_result):
    if len(str_result) > 12:
        formatted_output = str_result[:12] + "..."
    else:
        formatted_output = str_result
    return formatted_output

def perc_output(cocoa):
    formatted_output = str(cocoa).replace(".0", "%")
    return formatted_output

def digi_output(rating):
    formatted_output = "{0:.1f}".format(rating, 1)
    return formatted_output


#process commands
def process_command(command):
    command_list = command.lower().split()

    command_dict = {
        "specification":"",
        "keyword":"",
        "criteria": "ratings",
        "sorting":"top",
        "limit":"10",
        "sources": "sellers"
    }

    command_type_list = ["bars", "companies", "countries", "regions"]
    criteria_list = ["ratings", "cocoa", "bars_sold"]
    sorting_list = ["top", "bottom"]
    sources_list = ["sellers", "sources"]
    specification_list = ["sellcountry", "sourcecountry", "sellregion", "sourceregion", "country", "region", "sellers", "sources"]

    implement = True

    for command in command_list:
        if command in command_type_list:
            command_dict["command_type"] = command
        elif command in criteria_list:
            command_dict["criteria"] = command
        elif command in sources_list:
            command_dict["sources"] = command
        elif "=" in command:
            spec_list = command.split("=")
            for item in spec_list:
                if item in sorting_list:
                    command_dict["sorting"] = spec_list[0]
                    command_dict["limit"] = spec_list[1]
                elif item in specification_list:
                    if spec_list[0] == "sellcountry":
                        command_dict["specification"] = "C1.Alpha2"
                    elif spec_list[0] == "sourcecountry":
                        command_dict["specification"] = "C2.Alpha2"
                    elif spec_list[0] == "sellregion":
                        command_dict["specification"] = "C1.Region"
                    elif spec_list[0] == "sourceregion":
                        command_dict["specification"] = "C2.Region"
                    elif spec_list[0] == "country":
                        command_dict["specification"] = "Alpha2"
                    else:
                        command_dict["specification"] = spec_list[0].title()
                    command_dict["keyword"] = spec_list[1].title()
        else:
            implement = False
            print("Invalid Input or Exit. ")

    results = []
    # execute bars_command
    if command_dict["command_type"] == "bars" and implement == True:
        results = bars_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"])
        #print(results)
        # 6-tuple: 'SpecificBeanBarName','Company', 'CompanyLocation', 'Rating', 'CocoaPercent', 'BroadBeanOrigin'
        row_format = "{0:16} {1:16} {2:16} {3:6} {4:6} {5:16}"
        for row in results:
            #print(row)
            try:
                print(row_format.format(str_output(row[0]), str_output(row[1]), str_output(row[2]), digi_output(row[3]),perc_output(row[4]), str_output(row[5])))
            except:
                print(row_format.format(str_output(row[0]), str_output(row[1]), str_output(row[2]), digi_output(row[3]),perc_output(row[4]), "unknown"))

        return results

    elif command_dict["command_type"] == "companies" and implement == True:
        results = companies_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"])

        # 3-tuptle: 'Company', 'CompanyLocation', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16} {2:16}"
        for row in results:
            agg = ""
            if command_dict["criteria"] == "ratings":
                agg = digi_output(row[2])
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(row[2])
            elif command_dict["criteria"] == "bars_sold":
                agg = row[2]

            # print(row[0])
            print(row_format.format(str_output(row[0]), str_output(row[1]),agg))

        return results

    elif command_dict["command_type"] == "countries" and implement == True:
        results = countries_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"], command_dict["sources"])

        # 3-tuptle: 'Country', 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16} {2:16}"
        for row in results:
            (ct, r, agg) = row
            if command_dict["criteria"] == "ratings":
                agg = digi_output(agg)
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(agg)

            print(row_format.format(str_output(ct), str_output(r),agg))

        return results

    elif command_dict["command_type"] == "regions" and implement == True:
        results = regions_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"], command_dict["sources"])

        # 2-tuptle: 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16}"
        for row in results:
            (r, agg) = row
            if command_dict["criteria"] == "ratings":
                agg = digi_output(agg)
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(agg)

            print(row_format.format(str_output(r), agg))

        return results


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'help':
            print(help_text)
            continue
        elif response == 'exit':
            break
        try:
            results = process_command(response)
        except:
            continue




# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    create_table_Countries()
    create_table_Bars()
    interactive_prompt()
