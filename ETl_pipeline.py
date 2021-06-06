"""
Program to read the given txt file and convert it to a csv format.
Extracts only the mentioned columns and writes in a csv file using python inbuild libraries.
Uses Python 3.5.
"""

import csv

def one_hot_encoding(row_to_add):
    """
    Have used the one-hot-encoding and this can be easily decoded.
    :param row_to_add: Contains the rows from the input file.
    :return: An integer value for the encoded engine location values.
    """

    enginelocation_values = ['front', 'rear']

    char_to_int = dict((c, i) for i, c in enumerate(enginelocation_values))
            
    for value in row_to_add:
        if value == "engine-location":
            val = [v for k,v in row_to_add.items() if value in k ]
            integer_encoded = [v for k,v in char_to_int.items() if val[0] in k ]                
            letter = [0 for _ in range(len(enginelocation_values))]
            letter[integer_encoded[0]] = 1

            strings = [str(integer) for integer in letter]
            a_string = "".join(strings)
            letter_an_integer = int(a_string)

    return letter_an_integer

def wordtonum(row_to_add):
    """
    :param row_to_add: Contains the rows from the input file.
    :return: int type values, converted words to numbers.
    """

    word_dict = { 'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5', 'six': '6',
                  'seven': '7', 'eight': '8', 'nine': '9', 'zero' : '0' }

    numberof_cylinders = [v for k,v in row_to_add.items() if 'num-of-cylinders' in k]
    string_number =  ''.join(numberof_cylinders)

    # Convert numeric words to numbers
    # Using join() + split()
    res = int(''.join(word_dict[ele] for ele in string_number.split()))

    return res


def aspiration_value(row_to_add):
    """
    :param row_to_add: Contains the rows from the input file.
    :return: int type aspitaion_value in boolean values.
    """

    aspitaion_value = [v for k,v in row_to_add.items() if 'aspiration' in k]
    aspiration_string = ''.join(aspitaion_value)

    aspiration = int(1 if 'turbo' in aspiration_string else 0)

    return aspiration


def price_conversion(row_to_add):
    """
    :param row_to_add: Contains the rows from the input file.
    :return: Float type price value and in euros.
    """

    price_value = [v for k,v in row_to_add.items() if 'price' in k]
    price = float(price_value[0]) /100

    return price


def required_columns(row_to_add, wanted_keys):
    """
    :param row_to_add: Contains the rows from the input file.
    :param wanted_keys: List of required column header names to be displayed at the end.
    :return: Dict of keys,values formatted data.
    """

    required_keys = dict((k, row_to_add[k]) for k in wanted_keys if k in row_to_add)
            
    return required_keys


def output_convert(rows_selected):
    """

    :param rows_selected: Contains the rows from the input file.
    :return: list of list formatted output rows.
    """

    return [list(col) for col in zip(*[d.values() for d in rows_selected])]

def ETL_pipeline():
    """
    Writes the data into teh csv file after passing through the pipeline of data cleaning and formatting.
    """
    # The keys we want
    wanted_keys = ['engine-location', 'num-of-cylinders', 'engine-size', 
                   'weight', 'horsepower' ,'aspiration', 'price', 'make']

    rows_selected = []

    # read the input file
    with open('Challenge_me.txt', 'r') as file:
        headers = next(file).split(';')
        for row in file:
            if '-;' in row or '- ' in row:
                pass
            else:
                row_formated = row.split(';')

                row_to_add = dict(zip(headers,row_formated))

                row_to_add['engine-location'] = one_hot_encoding(row_to_add)

                row_to_add['num-of-cylinders'] = wordtonum(row_to_add)

                row_to_add['aspiration'] = aspiration_value(row_to_add)

                row_to_add['price'] = price_conversion(row_to_add)

                # formatting the values
                row_to_add['engine-size'] = int(row_to_add['engine-size'])
                row_to_add['weight'] = int(row_to_add['weight'])
                row_to_add['horsepower'] = float(row_to_add['horsepower'].replace(',', '.'))

                selected_columns = required_columns(row_to_add, wanted_keys)       
                
                rows_selected.append(selected_columns)

        covert_final_output_format = output_convert(rows_selected)
        covert_final_output_format.insert(0, wanted_keys)
                
        # write to the output csv file
        with open('Output.csv', mode='w', encoding='utf-8') as f:
            out_writer = csv.writer(f, delimiter=',')
            out_writer.writerow([covert_final_output_format])

            print(len(rows_selected))

if __name__ == "__main__":
    ETL_pipeline()