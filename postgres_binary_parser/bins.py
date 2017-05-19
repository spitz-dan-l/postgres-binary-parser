import pandas as pd

def reduce_categories(mapping_func, col, *, vectorized=False):
    if len(col.cat.categories) == 0:
        return col.copy()

    input_categories = pd.Series(col.cat.categories)
    try:
        nan_bin = mapping_func(pd.np.nan)

        # Handle the case where mapping function returns the string 'nan'
        if nan_bin == 'nan':
            nan_bin = pd.np.nan

    except:
        nan_bin = pd.np.nan

    if vectorized:
        output_categories = mapping_func(input_categories)
    else:
        output_categories = input_categories.map(mapping_func)

    unique_output_cats = pd.Index(output_categories.dropna())

    if not pd.isnull(nan_bin):
        unique_output_cats = unique_output_cats.append(pd.Index([nan_bin]))

    unique_output_cats = unique_output_cats.drop_duplicates()

    def index_or_neg1(x, get_loc=unique_output_cats.get_loc, KeyError=KeyError):
        try:
            return get_loc(x)
        except KeyError:
            return -1

    output_indexes = output_categories.map(index_or_neg1)
    output_codes = output_indexes.reindex(col.cat.codes).fillna(-1).astype('int64')

    if not pd.isnull(nan_bin):
        output_codes.replace(-1, unique_output_cats.get_loc(nan_bin))

    output_codes.index = col.index
    return pd.Categorical.from_codes(output_codes, unique_output_cats)
