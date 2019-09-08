import csv
import sys
import re
from collections import OrderedDict

###################### globals ##########################
DB_DIR = "./files/"
META_FILE = "./files/metadata.txt"
AGGREGATE = ["min", "max", "sum", "avg", "count", "distinct"]


###################### functions ##########################

def make_schema(filename):  
    ''' read the schema into the dict '''
    try:
        out = {}
        flag = 0
        
        with open(filename) as f:
            temp = f.readlines()
        
        for x in temp:
            if x.strip() == "<end_table>":
                flag = 0
                cur = ""
            
            if flag == 1:
                cur = x.strip()
                out[cur] = []
                flag = 2
            elif flag == 2:
                out[cur].append(x.strip())

            if x.strip() == "<begin_table>":
                flag = 1  

        return out
    except:
        sys.exit("Couldn't read metadata file.")

def populate_fields(tName):
    try:
        fetched_data = []
        # print("in read file")
        with open(tName,'r') as f:
            reader = csv.reader(f)
            for row in reader:
                fetched_data.append(row)

        return fetched_data
    except:
        sys.exit("Couldn't open file")

def join_tables(c_names,t_names,schema):

    print("in join_tables ")
    try:
        # t_names.reverse()
        t_names[0],t_names[1] = t_names[1],t_names[0]

        schema["basic"] = schema[t_names[1]] + schema[t_names[0]]

        schema["full"] = []
        for i in schema[t_names[0]]:
            schema["full"].append(t_names[0] + '.' + i)

        for i in schema[t_names[1]]:
            schema["full"].append(t_names[1] + '.' + i)

        fetched_data = []
        list1 = populate_fields(DB_DIR + t_names[0] + '.csv')
        list2 = populate_fields(DB_DIR + t_names[1] + '.csv')
        t_names.remove(t_names[0])
        t_names.remove(t_names[0])
        t_names.insert(0,"full")
        
        for item1 in list1:
            for item2 in list2:
                fetched_data.append(item2 + item1)

        if(c_names[0] == '*' and len(c_names) == 1):
            c_names = schema[t_names[0]]

        # print header
        for i in range(len(c_names)):
            print(c_names[i], end=" ")
        print("\n")
        print("schema[basic]", schema['basic'])
        for data in fetched_data:
            for col in c_names:
                if '.' in col:
                    print(data[schema[t_names[0]].index(col)], end = "\t")
                elif schema['basic'].count(col) > 1:
                    sys.exit("\nError: Column occurs in more than one table. Please flag!\n\n")
                else:
                    print(data[schema["basic"].index(str(col))], end = "\t")
            print("\n")

        del schema['full']
        del schema['basic']

    except:
        sys.exit("Error! Please flag syntax")

def processQuery(query,schema):
    # print("in processQuery")
    # try:
    if "from" not in query:
        sys.exit("Incorrect Syntax: No FROM keyword exists in query.")
    
    query = (re.sub(' +',' ',query)).strip();
    query = query.split(';')
    query = query[0]

    # else:
    
    item_1 = query.split('from');

    # removing the space before "from" in query
    item_1[0] = (re.sub(' +',' ',item_1[0])).strip(); 
    items = []
    items.append(0)

    if "select" not in item_1[0].lower():
        sys.exit("Incorrect Syntax: query should begin with the SELECT keyword.")
    items.append(item_1[0][7:]) # appending the part after select to items

    items[1] = (re.sub(' +',' ',items[1])).strip();
    l = []
    l.append("select")
    if "distinct" in items[1]:
        items[1] = items[1][9:]
        l.append("distinct")
    l.append(items[1])
    items[1] = l 

    item_new = ""
    if "distinct" in items[1][1]:
        item_new = items[1][1];
        item_new = (re.sub(' +',' ',item_new)).strip()
        items[1][1] = items[1][2]
    
    item_1[1] = (re.sub(' +',' ',item_1[1])).strip();

    temp = item_1[1].split('where');
    colStr = items[1][1];
    colStr = (re.sub(' +',' ',colStr)).strip()
    items.append(temp)
    c_names = colStr.split(',');
    # storing the column names in c_names after removing spaces
    for i in range(len(c_names)):
        c_names[c_names.index(c_names[i])] = (re.sub(' +',' ',c_names[i])).strip();
    
    tableStr = items[2][0]
    tableStr = (re.sub(' +',' ',tableStr)).strip();
    t_names = tableStr.split(',')
    for i in range(len(t_names)):
        t_names[t_names.index(t_names[i])] = (re.sub(' +',' ',t_names[i])).strip();
    for i in range(len(t_names)):
        if t_names[i] not in schema.keys():
            sys.exit("Error: Table doesn't exist! Please flag.")

    res = [i for i in c_names if '(' in i]

    if len(t_names) == 1:
        if len(items[2]) > 1:
           
            items[2][0] = (re.sub(' +',' ',items[2][0])).strip()
            items[2][1] = (re.sub(' +',' ',items[2][1])).strip()
            processWhere(items[2][1],c_names,t_names,schema)
           
            return
    else:
        if len(items[2]) > 1 and len(res) == 0:
            items[2][0] = (re.sub(' +',' ',items[2][0])).strip()
            items[2][1] = (re.sub(' +',' ',items[2][1])).strip()
            processWhereJoin(items[2][1],c_names,t_names,schema)
           
            return

        elif len(items[2]) > 1 and len(res) == 1:
           
            items[2][0] = (re.sub(' +',' ',items[2][0])).strip()
            items[2][1] = (re.sub(' +',' ',items[2][1])).strip()
            if not len(c_names) == 1:
                sys.exit("Incorrect syntax!\n")
            names = []
            a1 = c_names[0].split('(')
            names.append((re.sub(' +',' ',a1[0])).strip())
            names.append(a1[1][0])
            if names[1] not in schema[t_names[0]] and names[1] not in schema[t_names[1]]:
                sys.exit("Error: Column doesn't exist in the table\n")
            elif names[1] == '*':
                sys.exit("Error: Please flag sytax!\n")
            
            whereJoinAggregate(items[2][1],names[0],names[1],t_names,schema)
            
            return

        join_tables(c_names,t_names,schema)
        return

    if item_new == "distinct":
        # print("here")
        distinctMany(c_names,t_names,schema)
        return
    
    if len(c_names) == 1:
        if '(' in c_names[0] and ')' in c_names[0]:
            names = []
            a1 = c_names[0].split('(')
            names.append((re.sub(' +',' ',a1[0])).strip())
            names.append(a1[1][0])
            if names[1] not in schema[t_names[0]]:
                sys.exit("Error: Column doesn't exist in the table\n")
            if names[1] == '*':
                sys.exit("Error: Please flag sytax!\n")
            colList = []
            tName = DB_DIR + t_names[0] + '.csv'
            fetched_data = populate_fields(tName)
            for data in fetched_data:
                colList.append(int(data[schema[t_names[0]].index(names[1])]))
            aggregate(names[0],colList,t_names[0],schema)
            return

        elif '(' in c_names[0] or ')' in c_names[0]:
            sys.exit("Syntax error")

    selectColumns(c_names,t_names,schema);
    # except:
    #     sys.exit("Error! Please flag syntax")

def whereJoinAggregate(condition, operation, col,t_names,schema):
    try:
        whr = condition.split(" ")
        fetched_data = []
        schema["full"] = []
        list1 = []
        list2 = []
        t_names[0], t_names[1] = t_names[1], t_names[0]
        
        for i in range(len(schema[t_names[0]])):
            schema["full"].append(t_names[0] + '.' + schema[t_names[0]][i])

        for i in range(len(schema[t_names[1]])):
            schema["full"].append(t_names[1] + '.' + schema[t_names[1]][i])
        schema["basic"] = schema[t_names[1]] + schema[t_names[0]]
        list1 = populate_fields(DB_DIR + t_names[0] + '.csv')

        list2 = populate_fields(DB_DIR + t_names[1] + '.csv')
        for item1 in list1:
            for item2 in list2:
                fetched_data.append(item2 + item1)
        
        t_names.remove(t_names[0])
        t_names.remove(t_names[0])
        t_names.insert(0,"full")

        print(col,"\n")

        colList = []
        for data in fetched_data:
            check_cond = evaluate(whr,t_names,schema,data)
            if eval(check_cond):
                if '.' in col:
                    colList.append(data[schema[t_names[0]].index(col)])
                elif schema["basic"].count(col) > 1:
                    sys.exit("Error: Column exists in both the columns. Please specify as table_name.col_name")
                else:
                    colList.append(data[schema["basic"].index(col)])

        aggregate(operation,colList,t_names,schema)

        del schema['full']
        del schema['basic']
    except:
        sys.exit("Error: Please flag syntax.")

def divide_chunks(l, n): 
       
    for i in range(0, len(l), n+1):  
        yield l[i:i + n] 

    return l

def evaluate(whr,t_names,schema,data):

    check_cond = ""
    # check_cond.append("")

    for i in whr:
        if i.isalpha():
            for j in schema[t_names[0]]:
                if i in j:
                    whr[whr.index(i)] = j

    # whr = list(divide_chunks(whr, 3))
    for i in whr:
        if i in schema[t_names[0]] :
            check_cond += data[schema[t_names[0]].index(i)]
        elif i.lower() == 'and' or i.lower() == 'or':
            check_cond += ' ' + i.lower() + ' '
        elif i == '=':
            check_cond += i*2
        else:
            check_cond += i

    return check_cond


def selectColumns(c_names,t_names,schema):
    try:
        if c_names[0] == '*' and len(c_names) == 1:
            c_names = schema[t_names[0]]

        tName = DB_DIR + t_names[0] + '.csv'
        fetched_data = populate_fields(tName)
        
        for i in c_names:
            if i not in schema[t_names[0]]:
                sys.exit("Error: Field doesn't exist for this table. Please flag the query.")

        print_cols(c_names,t_names,schema)
        print_res(fetched_data,c_names,t_names,schema)
    except:
        sys.exit('Error: Check syntax')

def processWhere(condition,c_names,t_names,schema):
    try:
        whr = condition.split(" ")
        if(len(c_names) == 1 and c_names[0] == '*'):
            c_names = schema[t_names[0]]

        fetched_data = populate_fields(DB_DIR + t_names[0] + '.csv')

        flag = False
        x = c_names[0]
        if('(' in x and ')' in x):
            names = []
            a1 = x.split('(')
            a1[1] = (re.sub(' +',' ',a1[1])).split(')')
            del(a1[1][1])
            names.append((re.sub(' +',' ',a1[0])).strip())
            c_names = a1[1]
            print_cols(c_names,t_names,schema)
            tName = DB_DIR + t_names[0] + '.csv'
            fetched_data = populate_fields(tName)
            x_data = []
            for data in fetched_data:
                check_cond = evaluate(whr,t_names,schema,data)
                for col in c_names:
                    if eval(check_cond):
                        x_data.append(data[schema[t_names[0]].index(col)])
            aggregate(a1[0], x_data, t_names[0], schema)
            return

        elif '(' in x or ')' in x:
            sys.exit("Please flag syntax.")

        else:
            print_cols(c_names,t_names,schema)

            for data in fetched_data:
                check_cond = evaluate(whr,t_names,schema,data)
                for col in c_names:
                    if eval(check_cond):
                        flag = 'checked'
                        print(data[schema[t_names[0]].index(col)],end="\t")
                if flag:
                    flag = False
                    print("\n")
    except:
        sys.exit("Error! Please flag syntax")

def processWhereJoin(condition,c_names,t_names,schema):
    try:
        list1 = []
        list2 = []
        t_names.reverse()
        
        fetched_data = []
        
        whr = condition.split(" ")

        list1 = populate_fields(DB_DIR + t_names[0] + '.csv')
        list2 = populate_fields(DB_DIR + t_names[1] + '.csv')
        for item1 in list1:
            for item2 in list2:
                fetched_data.append(item2 + item1)
        schema["full"] = []
       
        for i in range(len(schema[t_names[1]])):
            schema["full"].append(t_names[1] + '.' + schema[t_names[1]][i])
        for i in range(len(schema[t_names[0]])):
            schema["full"].append(t_names[0] + '.' + schema[t_names[0]][i])

        schema["basic"] = schema[t_names[1]] + schema[t_names[0]]
        t_names.remove(t_names[0])
        t_names.remove(t_names[0])
        t_names.insert(0,"full")

        flag = False
        if(len(c_names) == 1 and c_names[0] == '*'):
            c_names = schema[t_names[0]]


        for i in c_names:
            print(i, end="\t")
        print("\n")

        for data in fetched_data:
            check_cond = evaluate(whr,t_names,schema,data)
            for col in c_names:
                if eval(check_cond):
                    flag = 'checked'
                    if '.' in col:
                        print(data[schema[t_names[0]].index(col)], end="\t")
                    elif schema["basic"].count(col) > 1:
                        sys.exit("Error: Column exists in both the columns. Please specify as table_name.col_name")
                    else:
                        print(data[schema["basic"].index(col)],end="\t")
            if flag:
                flag = False
                print("\n")
            else:
                pass

        del schema['full']
        del schema['basic']
    except:
        sys.exit("Error! Please flag syntax.\n")

def aggregate(func,colList,t_name,schema):
# AGGREGATE = ["min", "max", "sum", "avg", "count", "distinct"]

    try:
        for i in range(len(colList)):
            colList[i] = int(colList[i])
    except:
        sys.exit("error")
    if len(colList) == 0:
        print("\n")
    else:
        if func.lower() == AGGREGATE[2]:
            print(sum(colList))
        elif func.lower() == AGGREGATE[3]:
            print(sum(colList)/len(colList))
        elif func.lower() == AGGREGATE[5]:
            distinct(colList,c_name,t_name,schema);
        elif func.lower() == AGGREGATE[1]:
            print(max(colList))
        elif func.lower() == AGGREGATE[0]:
            print(min(colList))
        else :
            print("Error: Check the aggregate func!\n")

def distinct(colList,c_name,t_name,schema):
    try:
        check_cond = t_name + '.' + c_name
        print(str(check_cond))
        colList = list(OrderedDict.fromkeys(colList))
        size_colList = len(colList)
        for col in range(size_colList):
            print(colList[col],"\n")
    except:
        sys.exit("Error! Please flag syntax.\n")

def distinctMany(c_names,t_names,schema):
    temp = []
    flag = False
    print_cols(c_names,t_names,schema)
    try:
        if len(c_names) == 1 and len(t_names) == 1:
            for tab in t_names:
                t_name = DB_DIR + tab + '.csv'
                with open(t_name,'r') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        for col in c_names:
                            x_read = [""]
                            x_read[0] = row[schema[t_names[0]].index(col)]
                            if x_read[0] not in temp:
                                temp.append(x_read[0])
                                flag = 'checked'
                                print(x_read[0], end = "\t  ")
                        if not flag:
                            pass
                        else:
                            flag = True
                            print("\n")
        else:
            temp = [[]]
            l = len(c_names)
            ctr = 0
            for tab in t_names:
                tName = tab + '.csv'
                with open(tName,'r') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        x_read = [[0]*l]
                        for col in c_names:
                            x_read[0][ctr] = row[schema[tab].index(col)]
                            ctr = ctr + 1
                            ctr = ctr % l
                        if x_read[0] not in temp:
                            temp.append(x_read[0])
                            print(*x_read[0],sep="\t  ")
                            print("\n")
    except:
        sys.exit("Error! Please flag syntax")

def print_cols(c_names,t_names,schema):
    try:
        check_cond = []
        for col in c_names:
            for tab in t_names:
                if col in schema[tab]:
                    check_cond.append(tab + '.' + col)
        print(*check_cond, sep="  ")
    except:
        sys.exit("Error! Please flag syntax")

def print_res(fetched_data,c_names,t_names,schema):
    try:
        for data in fetched_data:
            for col in range(len(c_names)):
                print(data[schema[t_names[0]].index(c_names[col])], end="\t")
            print("\n")
    except:
        sys.exit("Error! Please flag syntax")


def main(query):
    schema = make_schema(META_FILE)
    processQuery(query, schema)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("USAGE : python3 {} '<sql query>'".format(sys.argv[0]))
        exit(-1)
    q = str(sys.argv[1])
    main(q)