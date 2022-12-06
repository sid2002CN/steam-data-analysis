# ETL codes
1. Get all the game id, title, url, current price, current discount whose popularity > 500 on SteamPrice History.
   1. iterate through pages from 1 to 92 (because after 92 the game popularity is less than 500) and as a keyword in api: ```url = "https://steampricehistory.com/popular?page="```
   2. find all games on each page's game table and collect their information into a dataframe:
   ```python
   for page in range(1,93): #92 for the popularity > 500
    result = requests.get(url+str(page), headers=headers)
    soup = BeautifulSoup(result.content)
    table = soup.find("table",{"class":"app-table"})
    trs = table.find_all("tr")
    for tr in trs[1:]:
        dic = {}
        tds = tr.find_all("td")
        dic["title"] = tds[1].text.strip()
        dic["url"] = tr.find("a")["href"]
        dic["price"] = tds[2].text[1:]
        dic["discount"] = tds[3].text
        dic["popularity"] = tds[4].text
        lst.append(dic)
    time.sleep(1)
    general = pd.DataFrame(lst)
   ```
   3. Since each game's id is the last number in their url, define a new function to split and get the id of those games, add a new column (id) in the general dataframe.
   ```python
   def set_id(row):
    index = row.find("app/")
    return row[index+4:]
   
    general["id"] = general["url"].apply(set_id)
   
    general = general.set_index("id")
    general.to_csv(path+"general.csv")
   ```
   4. After getting all urls of each game, we could enter each url and scrape all history discounts and sales information of each game, and save each of the game's discounts and sales as individual files.
   ```python
   for url in urls:
    discounts = []
    sales = []
    result = requests.get(url, headers=headers)
    soup = BeautifulSoup(result.content)
    tables = soup.find_all("section", {"class": "breakdown"})
   
    # only discounts and sales
    if len(tables) == 2:
        #table1: discounts
        table_discount = tables[0]
        trs = table_discount.find_all("tr")
        for tr in trs[1:]:
            dic = {}
            tds = tr.find_all("td")
            dic["date"] = tds[0].text
            dic["price"] = tds[1].text
            dic["gain"] = tds[2].text
            dic["discount"] = tds[3].text
            discounts.append(dic)
   
        #table2: sales
        table_sales = tables[1]
        trs = table_sales.find_all("tr")
        for tr in trs[1:]:
            dic = {}
            tds = tr.find_all("td")
            dic["sale"] = tds[0].text
            dic["date start"] = tds[1].text
            dic["price"] = tds[2].text
            dic["discount"] = tds[3].text
            sales.append(dic)
   
        d_file = pd.DataFrame(discounts)
        s_file = pd.DataFrame(sales)
   
        name = general.loc[general['url'] == url].index.item()
        d_file.to_csv(path+name+"_discounts.csv")
        s_file.to_csv(path+name+"_sales.csv")
        time.sleep(1)
   ```

2. Scrape games genres and categories(tags) using Steam Official API.
   1. iterate through all games id and as a keyword in api: ```http://store.steampowered.com/api/appdetails?appids={app_id}```
   2. detect and resist Steam block system by using:
    ```python
     while page == None:
        print("Sleeping....")
        time.sleep(60)
        page = requests.get(f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()
    ```
    if the return page is None, then sleep for 1 min and try to get the page again.
    3. if there is no such game_id in Steam Storage, add this game into the error_list and continue:
   ```python
    if page[app_id]["success"] == False:
        error_lst.append(app_id)
        continue
   ```
    4. if the page is return successfully, then try to find if there is genre and category keys, if there is, store all the genres and categories of this game in a new dictionary, store this dictionary in the final list. Also, if the category name or the genre name is not stored before, update the category/genre name lists (for here, cat and gen)
   ```python
   if "genres" in keys and "categories" in keys:
   
        #get all categories of the app
        categories = page["categories"]
        cat_lst = []
        for category in categories:
            cat_id = category["id"] 
            cat_name = category["description"] 
   
            #update category_index.csv
            if cat_id not in cat["id"]:
                cat["id"].append(cat_id)
                cat["name"].append(cat_name)
            
            cat_lst.append(str(cat_id))
   
        #get all genres of the app
        genres = page["genres"]
        gen_lst = []
        for genre in genres:
            gen_id = genre["id"] 
            gen_name = genre["description"] 
   
            #upload genre_index.csv
            if gen_id not in gen["id"]:
                gen["id"].append(gen_id)
                gen["name"].append(gen_name)
            
            gen_lst.append(str(gen_id))
        
        dic["id"] = app_id
        dic["category"] = ",".join(cat_lst)
        dic["genre"] = ",".join(gen_lst)
        
        result.append(dic)
   ```
    5. There may also have the case that the game does not have any category but must have genre, then input null in category but keep its genre.
   ```python
    elif "genres" in keys:
        genres = page["genres"]
        gen_lst = []
        for genre in genres:
            gen_id = genre["id"] 
            gen_name = genre["description"] 
   
            #upload genre_index.csv
            if gen_id not in gen["id"]:
                gen["id"].append(gen_id)
                gen["name"].append(gen_name)
            
            gen_lst.append(str(gen_id))
    
        dic["id"] = app_id
        dic["category"] = ""
        dic["genre"] = ",".join(gen_lst)
        
        result.append(dic)
   ```
   3. Else, there must be something wrong with this game, add to the error_list:
   ```python
   else:
        error_lst.append(app_id)
   ```
   4. Lastly, generate all files to csv: joint_category_genre stores all scraped genres and categories in joint.csv, category_index.csv includes all the ids, names of categories, genre_index.csv includes all the ids, names of genres.
    ```python
    final = pd.DataFrame(result)
    final.to_csv("joint_category_genre.csv", index = False)
    cat_df = pd.DataFrame.from_dict(cat)
    cat_df.to_csv("category_index.csv",index=False)
    gen_df = pd.DataFrame.from_dict(gen)
    gen_df.to_csv("genre_index.csv",index=False)
    ```


3.  Get all the game release date in leak.csv using Steam official API.
   1. iterate through all games id and as a keyword in api: ```http://store.steampowered.com/api/appdetails?appids={app_id}```
   2. detect and resist Steam block system by using:
    ```python
     while page == None:
        print("Sleeping....")
        time.sleep(60)
        page = requests.get(f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()
    ```
    if the return page is None, then sleep for 1 min and try to get the page again.
    3. if there is no such game_id in Steam Storage, add this game into the error_list and continue:
   ```python
    if page[app_id]["success"] == False:
        error_lst.append(app_id)
        continue
   ```
    4. if the page is return successfully, then try to find if there is "release_date" in keys, if there is, find the date and split by space to find the release year, append the dictionary in the final result list. If not, then add the game into the error list.
   ```python
   if "release_date" in keys:
        dic = {}
        date = page["release_date"]
        if date["coming_soon"] == True:
            coming.append(app_id)
        else:
            str_date = date["date"]
            lst = str_date.split(" ")
            year = lst[-1]
            dic["id"] = app_id
            # dic["day&month"] = md
            dic["year"] = year
            result.append(dic)
            print(dic)
    else:
        error_lst.append(app_id)
   ```
    5. Output time.csv that includes all ids and release year.
    ```python
    file = pd.DataFrame(result)
    file.to_csv("leak_cindy_result.csv", encoding="utf-8", index= False)
    ```
