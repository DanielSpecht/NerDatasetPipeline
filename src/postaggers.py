import polyglot
from polyglot.text import Text, Word

def polyglot_postagger(sentence):    
    """ 
    Annotates the sentence with its upostags
    Returns a dictionary with 2 keys:
     - tokens - List of the sentence tokens defined by the postagger
     - tags - List of postags of the words

    The first element of the 'tag' list is the annotation for the first element
    of the 'tokens' list
    """
    text = Text(sentence,hint_language_code="pt")
    tokens = [token for token, tag in text.pos_tags]
    tags = [tag for token, tag in text.pos_tags]
    return {"tokens":tokens,"tags":tags}

