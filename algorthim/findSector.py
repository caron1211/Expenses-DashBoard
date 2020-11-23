
from algorthim.googleSearch import GoogleSearch
from database.redis.myConnRedis import business_keys_db, business_names_db


def findSectorAlgor(business_name: str):
    if isInBusinessNamesDB(business_name) is not None:
        print("found business name")
        return isInBusinessNamesDB(business_name)

    if isInBusinessKeysDB(business_name) is not None:
        sector = isInBusinessKeysDB(business_name)
        business_names_db.insert(business_name, sector)
        print("found by keyword")
        # print(business_name, sector)
        return sector

    sector = findSectorByGoogle(business_name)
    # print(sector)
    business_names_db.insert(business_name, sector)

    return sector


def findSectorByGoogle(business_name: str):
    """Do a Google search for the business name and check if the title or keywords contain a keyword from
    BusinessKeysDB """
    google_search = GoogleSearch()
    title, snippet = google_search.search(business_name)

    if isInBusinessKeysDB(title) is not None:
        print("found by google search -title")
        return isInBusinessKeysDB(title)

    if isInBusinessKeysDB(snippet) is not None:
        print("found by google search -snippet")
        return isInBusinessKeysDB(snippet)

    print("fail in : ", business_name )

    return "fail"


def isInBusinessNamesDB(business_name: str):
    """Check if the business name have keyword in the business keys DB. if so, return the sector of the key.
    otherwise, return False """
    value = business_names_db.getValue(business_name)
    return value


def isInBusinessKeysDB(text: str):
    """ Check if the text contain keyword in the business keys DB. if so, return the sector of the key
    otherwise continue """
    value = business_keys_db.searchKeyInDB(text)
    return value