WANTED_COLUMNS_STOCKTWITS = ['_id_$oid',
                             'actor_followersCount',
                             'actor_id',
                             'actor_statusesCount',
                             'actor_tradingStrategy_approach',
                             'actor_tradingStrategy_assetsFrequentlyTraded',
                             'actor_tradingStrategy_experience',
                             'actor_tradingStrategy_holdingPeriod',
                             'body',
                             'entities_chart_url',
                             'entities_sentiment',
                             'entities_symbols',
                             'object_id',
                             'object_objectType',
                             'object_postedTime']

ID_FIELD_DF = '_id_$oid'
ID_FIELD = '$oid'
DATE_FIELD = 'object_postedTime'

ALL_COLUMNS_STOCKTWITS = ['id',
                          '_id_$oid',
                          'actor_classification',
                          'actor_displayName',
                          'actor_followersCount',
                          'actor_followingCount',
                          'actor_followingStocksCount',
                          'actor_id',
                          'actor_image',
                          'actor_link',
                          'actor_links',
                          'actor_objectType',
                          'actor_preferredUsername',
                          'actor_statusesCount',
                          'actor_summary',
                          'actor_tradingStrategy_approach',
                          'actor_tradingStrategy_assetsFrequentlyTraded',
                          'actor_tradingStrategy_experience',
                          'actor_tradingStrategy_holdingPeriod',
                          'body',
                          'entities_chart',
                          'entities_chart_large',
                          'entities_chart_original',
                          'entities_chart_thumb',
                          'entities_chart_url',
                          'entities_sentiment',
                          'entities_symbols',
                          'entities_video',
                          'id',
                          'inReplyTo_id',
                          'inReplyTo_objectType',
                          'link',
                          'object_id',
                          'object_link',
                          'object_objectType',
                          'object_postedTime',
                          'object_summary',
                          'object_updatedTime',
                          'verb']

UNWANTED_COLUMNS_STOCKTWITS = ['actor_followingStocksCount',
                               'inReplyTo_objectType',
                               'link',
                               'actor_preferredUsername',
                               'actor_image',
                               'object_summary',
                               'object_link',
                               'actor_objectType',
                               'actor_displayName',
                               'actor_followingCount',
                               'actor_classification',
                               'inReplyTo_id',
                               'id',
                               'actor_links',
                               'entities_chart',
                               'actor_link',
                               'actor_summary',
                               'entities_video',
                               'object_updatedTime']


WANTED_COLUMNS_WORDS = ['Word',
                        'Negative',
                        'Positive',
                        'Uncertainty']
