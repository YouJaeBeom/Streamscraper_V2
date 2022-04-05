def get_refresh_cursor(response_json):   
    """
    get refresh cursor value function
    """
    response_json = response_json

    try :
        ## get current cur range
        refresh_cursor = str(response_json['timeline']['instructions'][len(response_json['timeline']['instructions'])-2]['replaceEntry']['entry']['content']['operation']['cursor']['value'] )
    except :
        ## get current cur range
        refresh_cursor = str(response_json['timeline']['instructions'][0]['addEntries']['entries'][len(response_json['timeline']['instructions'][0]['addEntries']['entries'])-2]['content']['operation']['cursor']['value'])
    return refresh_cursor

def get_scroll_cursor(response_json):
    """ 
    get scroll cursor value function
    """
    response_json = response_json

    try :
        ## get current cur range
        scroll_cursor = str(response_json['timeline']['instructions'][len(response_json['timeline']['instructions'])-1]['replaceEntry']['entry']['content']['operation']['cursor']['value'] )
    except :
        ## get current cur range        
        scroll_cursor = str(response_json['timeline']['instructions'][0]['addEntries']['entries'][len(response_json['timeline']['instructions'][0]['addEntries']['entries'])-1]['content']['operation']['cursor']['value'])
    return scroll_cursor
