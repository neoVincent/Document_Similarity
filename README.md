# Document Similarity

## Work flow
- retrieve documents
- create a table with 6 columns, (document_id, word,type,word2vec,no_of_occurences,stemmed_word)
- perform certain NLP tasks
    - parse into words -- create a table and populate (document_id,word,'unknown_type',0,0,stemmed_word)
        - increment the no_of_occurrences every time a word occurs in a document 
    - remove stop words -- update those words -- unknown_type to 'STOP' -- these words will not be processed any further
    - perform stemming  -- this applies to words whose types 'unknown_type' -- set that 'stemmed_word'
    - lookup the word2vec embedding for the word and update the word2vec value for that document_id/word

## SpaCy

### Models & Languages
load the model
```python
python3 -m spacy download en_core_web_sm 
```
Get model info
```python
print(spacy.info("en_core_web_sd"))
```
> en_core_web_sm: doesn't come with a word vectors table
> en_core_web_md: has a relatively small number of vectors(between 10k and 20k)
## Useful links
