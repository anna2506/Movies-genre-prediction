
from random import random, randrange
from xmlrpc.client import MAXINT
from bs4 import BeautifulSoup
import numpy as np


import requests
import re
import codecs
import pandas as pd
import concurrent.futures

# Downloading imdb top 250 movie's data
REVIEW_ID_LENGTH = 7
N_THREADS = 4
ZEROS_STRING = "".join("0" for i in range(7))

url = 'http://www.imdb.com/title/tt'


#ids = pd.read_csv('Reviews_filtered.csv')

#ids = ids.movie_id.unique()

#print(len(ids))

APPEND_LIMIT = 100

data = []
total_reviews = 0


def scrap_titles(ids):

    with concurrent.futures.ThreadPoolExecutor(max_workers=N_THREADS) as executor:
        executor.map(scrap_title_by_id, ids)


def scrap_title_by_id(id):
    print(f"ID: {id}")
    title = origin_title = age_restriction = country_of_origin = duration = director = release_date = budget = gorss_worldwide = rating = number_of_ratings = user_reveiws = crtitic_reviews = None
    genres = []

    response = requests.get(
        url + ZEROS_STRING[:REVIEW_ID_LENGTH - len(str(id))] + str(id))
    print(
        f'{url + ZEROS_STRING[:REVIEW_ID_LENGTH - len(str(id))] + str(id)}    {response.status_code}')

    soup = BeautifulSoup(response.text, 'lxml')
    print(f"Response: {response}")
    write_file = open("soup.txt", "w")
    write_file.write(str(soup))

    title = soup.find(
        "h1", {"data-testid": "hero-title-block__title"})
    if title:
        title = title.text


    origin_title = soup.find(
        "div", {"data-testid": "hero-title-block__original-title"})
    if origin_title:
        origin_title = origin_title.text.split(": ")[1]

    # print(title)
    hero_title_block_metadata = soup.find(
        "ul", {"data-testid": "hero-title-block__metadata"})

    hero_title_block_metadata.children

    for i in hero_title_block_metadata.find_all("li"):

        if len(i.findChildren()) > 0:
            child = i.findChild().text
            if re.findall(r"\d{4}", child):
                continue
            if re.findall(r"\d{2}", child):
                # print(child)
                age_restriction = child
                continue
        elif re.findall(r"(m|h|s)", i.text):
            # print(i.text)
            duration = i.text
            continue

    ipc_metadata_list = soup.find(class_="ipc-metadata-list__item")

    ipc_metadata_list.ch

    item = ipc_metadata_list.findChild()
    # print(ipc_metadata_list)

    # print(item)
    if (item.text == 'Director'):
        director_class = ipc_metadata_list.findChildren()[1]
        director = director_class.findChildren()[-1].text
        # print(director)

    details = soup.find("div", {"data-testid": "title-details-section"})
    r = details.findChild()
    # print(r)
    rrr = r.findChild("li", {"data-testid": "title-details-releasedate"})
    if rrr:
        release_date = rrr.findChildren()[1].text
    # print(release_date)

    rrr = r.findChild("li", {"data-testid": "title-details-origin"})
    if rrr:
        country_of_origin = rrr.findChildren()[1].text
    # print(country_of_origin)

    box_office = soup.find("div", {"data-testid": "title-boxoffice-section"})
    if box_office:
        tmp_child = ""
        box_office = box_office.findChild()
        tmp_child = box_office.findChild(
            "li", {"data-testid": "title-boxoffice-budget"})
        if tmp_child:
            budget = tmp_child.findChildren()[1].text
        # print(budget)

        tmp_child = box_office.findChild(
            "li", {"data-testid": "title-boxoffice-cumulativeworldwidegross"})
        if tmp_child:
            gorss_worldwide = tmp_child.findChildren()[1].text
        # print(gorss_worldwide)

    hero_rating_bar = soup.find(
        "div", {"data-testid": "hero-rating-bar__aggregate-rating__score"})
    print(hero_rating_bar)


    rating = hero_rating_bar.findChild().text
    print(rating)
    number_of_ratings = hero_rating_bar.find_next_siblings()[-1].text
    print(f"Number of ratings: {number_of_ratings}")

    reviewContent_all_reviews = soup.find(
        "ul", {"data-testid": "reviewContent-all-reviews"})
    print(reviewContent_all_reviews)
    scores = reviewContent_all_reviews.find_all(
        #class_="less-than-three-Elements")
        class_="three-Elements")
    print(scores)

    for s in scores:
        children = s.findChildren()
        if children[1].text == "User reviews" or children[1].text == "User review":
            user_reveiws = children[0].text
            continue
        if children[1].text == "Critic reviews" or children[1].text == "Critic review":
            crtitic_reviews = children[0].text
            continue


    storyline_genres = soup.find("li", {"data-testid": "storyline-genres"})
    print(f"Genres LI {storyline_genres}")
    genres_li = storyline_genres.find_all("li")
    for g in genres_li:
        genres.append(g.text)
    #print("Genres: ", genres)

    film_description = soup.find("span", {"data-testid": "plot-xl"}).text
    print(f"Film description: {film_description}")

    print(str(id) + " " + str(title) + " " + str(origin_title) + " " + str(country_of_origin) + " " + str(age_restriction) + " " + str(duration) + " " + str(director) + " " + str(release_date) +
          " " + str(budget) + " " + str(gorss_worldwide) + " " + str(rating) + " " + str(number_of_ratings) + " " + str(user_reveiws) + " " + str(crtitic_reviews))

    print(f'Data length: {len(data)}')

    data.append((id, title, origin_title, country_of_origin, age_restriction, duration, director, release_date,
                budget, gorss_worldwide, rating, number_of_ratings, user_reveiws, crtitic_reviews, genres))

    
    if (len(data) == APPEND_LIMIT):
        df = pd.DataFrame(data)
        df.to_csv('Titles.csv', mode='a', index=False, header=False)
        data.clear()
    
    print('------------------------------------------------------')


#scrap_titles(ids)
scrap_title_by_id(120737)
