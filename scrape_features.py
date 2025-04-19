import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from concurrent.futures import ThreadPoolExecutor
from tornado import concurrent

def features_scrape(link):
    
    options = Options()
    service = Service()
    options.add_experimental_option("detach",True)
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options,service=service)
    try:
        driver.set_page_load_timeout(50)
        driver.get(link)
    except:
        driver.quit()
        print("Error time out.")
        return {}
    real_estate_info = {}
    real_estate_info.update({"URL":link})
    try:
        # Lấy thành phố (TPHCM, Hà Nội, Đà Nẵng)
        city = None
        city_name = driver.find_element(By.CSS_SELECTOR,'a.re__link-se[level="2"]')
        if city_name:
            city = city_name.text
        real_estate_info["City"] = city
        
        # Lấy quận/huyện
        district = None
        district_name = driver.find_elements(By.CSS_SELECTOR,'a.re__link-se[level="3"]')
        if len(district_name):
            district = district_name[0].text
        real_estate_info["District"] = district

        # Lấy loại bất động sản
        bds_type = None
        bds_type_name = driver.find_elements(By.CSS_SELECTOR,'a.re__link-se[level="4"]')
        if len(bds_type_name):
            bds_type = bds_type_name[0].text
        real_estate_info["Type"] = bds_type
        
        # Lấy tên bất động sản:
        ret_name = driver.find_element(By.CSS_SELECTOR, "h1.re__pr-title.pr-title.js__pr-title")
        ret = ret_name.text
        real_estate_info["Real Estate"] = ret

        # Lấy địa chỉ:
        address = None
        address_name = driver.find_elements(By.CSS_SELECTOR, "span.re__pr-short-description.js__pr-address")
        if len(address_name):
            address = address_name[0].text
        real_estate_info["Address"] = address

        # Lấy mức giá, diện tích, phòng ngủ (nếu là nhà riêng/căn hộ/ chung cư ...)
        price = None
        area = None
        priceperm2 = None
        areaperm2 = None
        rooms = None
        general_info = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-short-info-item.js__pr-short-info-item span.value")
        details = driver.find_elements(By.CSS_SELECTOR,"div.re__pr-short-info-item.js__pr-short-info-item span.ext")
        if len(general_info) > 0:
            price = general_info[0].text.strip()
            area = general_info[1].text.strip()
            if len(general_info) == 3:
                rooms = general_info[2].text.strip()
        if len(details) > 0:
            priceperm2 = details[0].text.strip()
            if len(details) == 2:
                areaperm2 = details[1].text.strip()
        real_estate_info.update({"Price":price, "Area": area, "Bedrooms": rooms, "PricePerM2": priceperm2, "AreaPerM2": areaperm2})

        # Lấy thông tin % tăng trong 1 năm qua
        increase_percent = None
        increase_percent_info = driver.find_elements(By.CSS_SELECTOR, "div.cta-number")
        if len(increase_percent_info) != 0:
            increase_percent = increase_percent_info[0].text.strip()
        real_estate_info["%Increse1year"] = increase_percent

        # Lấy thông tin mô tả
        description = None
        description_info = driver.find_elements(By.CSS_SELECTOR, "div.re__section-body.re__detail-content.js__section-body.js__pr-description.js__tracking")
        if description_info:
            description = description_info[0].text
        real_estate_info["Description"] = description
        
        # Lấy đặc điểm bất động sản
        other_info_title = [title.text for title in driver.find_elements(By.CSS_SELECTOR, "span.re__pr-specs-content-item-title")]
        other_info_value = [value.text for value in driver.find_elements(By.CSS_SELECTOR, "span.re__pr-specs-content-item-value")]
        for tt, vl in zip(other_info_title, other_info_value):
            real_estate_info.update({str(tt): vl})
        
        # Lấy các thông tin về vĩ độ, kinh độ =
        lati = None
        longi = None
        try:
            i = driver.find_element(By.CSS_SELECTOR, "iframe.lazyload")
            source = i.get_attribute("data-src")
            lat_long_part = source.split('q=')[1].split('&')[0]
            lati, longi = lat_long_part.split(',')
            real_estate_info['Latitude'] = lati
            real_estate_info['Longitude'] = longi
        except:
            print('Other info about latitude and longitude not found.')
        # Lấy các thông tin về ngày đăng, ngày hết hạn, loại tin, mã tin
        try:
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.re__pr-config")))
            i4 = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-short-info-item.js__pr-config-item span.title")
            i5 = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-short-info-item.js__pr-config-item span.value")
            other_info_title = [title.text for title in i4]
            other_info_value = [value.text for value in i5]
            for tt, vl in zip(other_info_title, other_info_value):
                real_estate_info.update({str(tt):vl})
        except:
            print("Other info about time posted not found.")
    except Exception as e:
        print(e)

    driver.quit()
    
    return real_estate_info

# Chạy multithreading
def multithrdg(links):
    count_links = 0
    ls_detailed_realestate = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(features_scrape,link) for link in links}
        for future in concurrent.futures.as_completed(futures):
            count_links += 1
            try:
                ls_detailed_realestate.append(future.result())
            except Exception as e:
                # print(f"Error scraping real estate feature at link: {count_links}, {futures[future]}")
                print(e)
    return ls_detailed_realestate

# Crawl nếu không chạy multithreading
def crawler(links):
    count_links = 0
    ls_detailed_realestate = []
    for link in links:
        count_links += 1
        in4 = features_scrape(link)
        if in4 != {}:
            ls_detailed_realestate.append(in4)
            print(f'✅ Done page {count_links}')
    return ls_detailed_realestate

start = time.time()

df_links = pd.read_csv("links.csv")
df = pd.DataFrame(crawler(df_links["Links to real estate"].tolist()[200:300]))

end = time.time()

print(len(df))
print(end - start)

df.to_csv("details200_299.csv", index=False)

