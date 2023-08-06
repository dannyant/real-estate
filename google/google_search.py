

from googlesearch import search
searchTerm = "3228 Fairview Court, Sacramento, CA site:apartments.com"
results = search(searchTerm, lang="en", advanced=True)
for i in results:
    print(i)

