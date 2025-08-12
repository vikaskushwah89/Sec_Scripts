import datetime

user_date = input("Please enter start date and time in the format DD-MM-YYYY HH:MM:SS:")

date_string = "Mon Apr 22 11:12:30 2024"

try:
    date_object_user = datetime.datetime.strptime(user_date, "%d-%m-%Y %H:%M:%S")
except ValueError:
    print("Please enter the date and time in the correct format (DD-MM-YYYY HH:MM:SS)")
else:
    date_object = datetime.datetime.strptime(date_string, "%a %b %d %H:%M:%S %Y")

    if date_object_user > date_object:
        print("You are in the future")
    elif date_object_user < date_object:
        print("You are in the past")
    else:
        print("You in the present")
