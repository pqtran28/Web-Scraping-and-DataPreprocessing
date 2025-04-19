from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import lxml

url = "https://batdongsan.com.vn/nha-dat-ban-{}?tpl=list&vrs=1"
base_url = "https://batdongsan.com.vn/nha-dat-ban-{}/p{}?vrs=1"

allRealEstate = []

def maxPage(link) -> int:
    options = Options()
    service = Service()
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options,service=service)
    driver.get(link)
    soup = BeautifulSoup(driver.page_source,'lxml')
    driver.quit()
    maxpage = soup.find_all("a", class_ = "re__pagination-number")[-1].text.strip()
    return int(maxpage)

def scrape(link, num_page):
    options = Options()
    service = Service()
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options,service=service)
    driver.get(link)

    soup = BeautifulSoup(driver.page_source,'lxml')
    driver.quit()

    for item in soup.find_all("a", class_ = "js__product-link-for-product-id"):
        href = item.get("href")
        if href:
            full_links = "https://batdongsan.com.vn" + href
            allRealEstate.append(full_links)
    
    print(f'✅ Done page {num_page}')

def all_Pages(prov):
    prov_url = url.format(prov)
    maxpage = maxPage(prov_url)
    scrape(prov_url,1)
    page = 2
    while page < maxpage:
        scrape(base_url.format(prov, page),page)
        page += 1
        
provinces = ['tp-hcm', 'ha-noi', 'da-nang']
for province in provinces:
    all_Pages(province)
df = pd.DataFrame(allRealEstate, columns = ['Links to real estate'])
print(len(df))
df.to_csv('D:/DS108/WEB_SCRAPING/BATDONGSAN/links.csv')

# TPHCM: 135 pages
# HÀ NỘI: 84 pages
# ĐÀ NẴNG: 14 pages
# TỔNG BĐS: 4677 bất động sản