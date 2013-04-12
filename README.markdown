# Tagging Portuguese Wikipedia

We want to tag Portuguese Wikipedia using [PyPLN](http://www.pypln.org/) and
[Palavras](http://visl.sdu.dk/~eckhard/pdf/PLP20-amilo.ps.pdf) (we have a
license). The goals of this project are:

- Release a part-of-speech tagged Portuguese Wikipedia Corpus under a Creative
  Commons license.
- Train a part-of-speech tagger on NLTK and release it under a free/libre
  software license.


## Assumptions

- We're going to use all Portuguese Wikipedia articles (pages).
- Probably we're going to use the Palavras' tagset, but we can then translate
  it to NLTK's tagset.
- We won't use an incremental tagger (the entire corpus will be loaded in
  memory to train a NLTK tagger).
- We still need to learn about trainers to decide which to use.


## Next Goals

- Split the entire corpus (and tagger) into Wikipedia Portals, so we'll have a
  tagged corpus by subject.
- Compare taggers (Palavras versus NLTK with our trained tagger)


## Related Links

- <http://dumps.wikimedia.org/backup-index.html>
- <http://dumps.wikimedia.org/ptwiki/20130306/>
- <http://stackoverflow.com/questions/1625162/get-text-content-from-mediawiki-page-via-api>
- <https://code.google.com/p/wikiteam/>
- <https://meta.wikimedia.org/wiki/Data_dumps/Download_tools>
- <https://meta.wikimedia.org/wiki/Data_dumps/Dump_format>
- <https://meta.wikimedia.org/wiki/Data_dumps/Other_tools>
- <https://meta.wikimedia.org/wiki/Data_dumps/Tools_for_importing>
- <https://meta.wikimedia.org/wiki/Data_dumps/What%27s_available_for_download>
- <https://meta.wikimedia.org/wiki/Data_dumps>
- <https://wikitech.wikimedia.org/wiki/Dumps#Code>
- <https://wikitech.wikimedia.org/wiki/Dumps#Worker_nodes>
- <https://www.mediawiki.org/wiki/API:Allpages>
- <https://www.mediawiki.org/wiki/API:Parsing_wikitext>
- <https://www.mediawiki.org/wiki/API:Query#Using_list.3Dallpages_as_generator>
