import requests
import time
import pandas as pd
import unicodecsv as csv
import os
import re
import random
import pathlib
import configparser
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from tqdm import tqdm
from multiprocessing import Pool, Process

#
config = configparser.ConfigParser()
config.read('config.ini')
path = config['file_detail']['folder_path']
# chromeDriver link
chrome_path =config['file_detail']['chrome_path']
file_name = config['file_detail']['file_name']
#temp_file = config['file_detail']['temp_file']
time_sleep = int(config['file_detail']['time_sleep'])
save_file= config['file_detail']['save_file']
consolidate_file=save_file
disable =  config['file_detail']['disable']
Short_type=config['file_detail']['Short_type']

df = pd.read_csv(file_name)
if 'complete' in df.columns:
    pass
else:
    df['complete']=" "
    df.to_csv(file_name,index=False)
df=pd.read_csv(file_name)

user_agent_list = [
    # Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
]

def crawl_data(driver,url):
    time.sleep(time_sleep+4)
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        toFind = re.compile("(bubble_[0-9]{2})+")
        v=soup.find_all("span", {"class":toFind})
        hotel_rating_differ=[]
        for i in v:
            r=i.get("class")
            hotel_rating_differ.append(r)
        r=hotel_rating_differ[2:6]
        custo=[]
        for i in r:
                we=(" ".join(i))
                custo.append(we)
        hotel_per=[]
        for i in custo:
            data=(i.split("_")[-1])
            hotel_per.append(data)
        location=hotel_per[0]
        clean=hotel_per[1]
        service=hotel_per[2]
        value_rate=hotel_per[3]
    except IndexError:
        location=""
        clean=""
        service=""
        value_rate=""
    #print(location,clean,service,value_rate)
    try:
        hotel_name=driver.find_element_by_css_selector(config['link_detail']['hotel_name_link'])       
        hotel_name=hotel_name.text
    except NoSuchElementException:
        hotel_name=" "
    try:
        hotel_address=driver.find_element_by_css_selector(config['link_detail']['hotel_address_link']).text
    except NoSuchElementException:
        hotel_address=""

    
    try:
        rating=driver.find_element_by_css_selector(config['link_detail']['hotel_rating_link']).text
        average=driver.find_element_by_css_selector(config['link_detail']['hotel_average_link']).text
    except NoSuchElementException:
        average=""
        rating=""
    total_review=""
    try:
        total_review = driver.find_element_by_css_selector(config['link_detail']['hotel_total_review_link']).text
    except NoSuchElementException:
        total_review=" "
    try:
        covid_detail=[]
        covid_safty=driver.find_elements_by_class_name(config['link_detail']['covid_detail_link'])
        for j in covid_safty:
            covid_detail.append(j.text)
        covid_detail=" ".join(covid_detail)    
    except IndexError:
        covid_detail=" "
    # covid_safty=driver.find_elements_by_class_name("_1R1TqoXf")
    rows=[]
    num_hotel_page =int(config['link_detail']['num_hotel_page'])
    # Start getting comments
    time.sleep(time_sleep+5)
    flag_page_over = False
    for i in (range(num_hotel_page)): #Page Loop
        try:
            try:
                read_more=driver.find_element_by_xpath(config['link_detail']['read_more_button_link'])
                driver.execute_script("arguments[0].click();", read_more)
                time.sleep(time_sleep+1)
            except NoSuchElementException:
                    pass
            
            review_list = driver.find_elements_by_xpath(config['link_detail']['hotel_review_list_link'])
            for i in review_list: # each comments in a single page
                row = {}
                # Set common data
                row['hotel_name']=hotel_name
                row['hotel_url']=url
                
                row['hotel_address']=hotel_address
                row['hotel_type']=rating
                row['hotel_rating']=average
                row['location_rating']=location
                row['cleaniness_rating']=clean
                row['service_rating']=service
                row['value_rating']=value_rate
                row['hotel_rating_max_value']=5
                row['hotel_review']=total_review
                row['covid_detail']=covid_detail
                
                try:
                    customer_review_date=i.find_element_by_css_selector(config['link_detail']['customer_review_date_link'])
                    customer_name= "".join(customer_review_date.text.split(" ")[0])
                    customer_review_date_value=customer_review_date.text
                    value=customer_review_date_value.split(" ")[-2:]
                    value=" ".join(value)
                except NoSuchElementException:
                    customer_name=" "
                    value=" "
                row['customer_name'] =customer_name
                try:
                    customer_address=i.find_element_by_css_selector(config['link_detail']['customer_review_address']).text
                except NoSuchElementException:
                    customer_address=" "
                row['customer_address'] =customer_address

                try:
                    title=i.find_element_by_css_selector(config['link_detail']['customer_review_title']).text
                    comment_title=title
                except  NoSuchElementException:
                    comment_title=" "
                try:
                    var = i.find_elements_by_xpath(config['link_detail']['customer_review_rating'])
                    data=var[0].get_attribute("class")
                    data = data.split("_")[-1]

                except IndexError:
                     data=" "
                try:
                    comment_data=i.find_elements_by_xpath(config['link_detail']['customer_review_comment'])
                    comment_date_v=comment_data[0].text
                except IndexError:
                    comment_date_v=""
                try:
                    trip_type=i.find_elements_by_class_name(config['link_detail']['customer_trip_type'])
                    trip_cause=trip_type[0].text
                except IndexError:
                    trip_cause=" "
                try:
                            date=i.find_elements_by_class_name(config['link_detail']['customer_stay_date'])
                            customer_stay_date=date[0].text
                except  IndexError:
                            customer_stay_date=" "
                row['customer_review_date']=value
            
                row['customer_rating']=data
                row['max_value']=50
                row['comment_title']=comment_title
                row['comment'] = comment_date_v
                row['customer_stay_date']=customer_stay_date
                row['customer_trip_type']=trip_cause

                
                rows.append(row)
            try:
                elem1 = driver.find_element_by_link_text(config['link_detail']['next_button'])
                driver.execute_script("arguments[0].click();", elem1)
                time.sleep(time_sleep)
            except NoSuchElementException:
                flag_page_over=True
                break
        
        except NoSuchElementException as e:
            print(e)
        if flag_page_over:
            break
    if len(rows)==0:
        pass
    else:
        keys = rows[0].keys()
        if not os.path.exists(save_file):
            with open(save_file, 'wb') as output_file:
                                dict_writer = csv.DictWriter(output_file, keys)
                                dict_writer.writeheader()
                                dict_writer.writerows(rows)
        else:
            with open(save_file,'ab') as output_file:
                                dict_writer = csv.DictWriter(output_file,keys)
                                #dict_writer.writeheader()
                                dict_writer.writerows(rows)

#
def complete_link(file_name,temp_link_file):
    temp_file=pd.read_csv(temp_link_file)
    data_file=pd.read_csv(file_name)
    hotel_link=temp_file['link']
    data_hotel_url=data_file['hotel_url']
    temp_hotel_link=[]
    for i in hotel_link:
        temp_hotel_link.append(i)
    data_hotel_link=[]
    for k in data_hotel_url:
        data_hotel_link.append(k)
    common = set(temp_hotel_link).intersection(data_hotel_link)
    index_value = [temp_hotel_link.index(x) for x in common]
    for i in common:
        temp_file['complete'].loc[temp_file['link']==i]='yes'
    temp_file.to_csv(temp_link_file,index=False)
     
def consolidate(file_name):
    df = pd.read_csv(file_name)
    print("Before Duplicate removed: ", df.size)
    df.drop_duplicates(inplace=True)
    print("After Duplicate removed: ", df.size)
    df['customer_stay_date'].replace(regex=True, inplace=True, to_replace=r'Date of stay:', value=r'')
    df['hotel_review'].replace(regex=True, inplace=True, to_replace=r' reviews', value=r'')
    df['customer_trip_type'].replace(regex=True, inplace=True, to_replace=r'Trip type:', value=r'')
    df.to_csv(file_name, mode="w", index=False)

def crawl(url):
    #print(url)
    options = Options()
    options.headless = True
    user_agent = random.choice(user_agent_list)
    options.add_argument(f'user-agent={user_agent}')
    check_file=df[df['link']==url]['complete'].values
    #time.sleep(time_sleep)
    #print(check_file)
    if (check_file[0]=="yes"):
        pass
    else:
        time.sleep(time_sleep)
        driver = webdriver.Chrome(executable_path=chrome_path,options=options)
        driver.get((url))
        time.sleep(time_sleep)
        if disable:
                #print("ert")
                input_data = driver.find_element_by_xpath("//input[@placeholder='Search reviews']")
                input_data.send_keys('disabled')
                time.sleep(time_sleep+12)
                input_data.send_keys(Keys.ENTER)
                time.sleep(time_sleep+10)
                crawl=crawl_data(driver,url)
                if os.path.isfile(save_file):
                    complete_link_file=complete_link(consolidate_file,file_name)
                driver.close()
        if Short_type: 
            check="none"
            try:
                jan_aug=driver.find_element_by_xpath( ".//*[contains(text(), 'Jun-Aug')]")
                driver.execute_script("arguments[0].click();", jan_aug)
                sep_nov=driver.find_element_by_xpath(".//*[contains(text(), 'Sep-Nov')]")
                driver.execute_script("arguments[0].click();", sep_nov)

                time.sleep(time_sleep)
                check="pass"
            except exceptions.StaleElementReferenceException:  
                        check="none"
            #print(check)
            if check=="pass":
                crawl_data(driver,url)
                if os.path.isfile(save_file):
                    complete_link_file=complete_link(consolidate_file,file_name)
                driver.close()
            if check=="none":
                    pass

if __name__ == "__main__":
    with Pool(2) as p:
       v=(p.map(crawl, df.link))
    #drop_link=complete_link(consolidate_file,file_name)
    if os.path.isfile(save_file):
        consolidate_data=consolidate(consolidate_file)