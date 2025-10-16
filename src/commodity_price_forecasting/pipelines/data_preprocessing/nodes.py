import pandas as pd

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df.drop_duplicates(subset=['date', 'unit', 'item'], inplace=True)

    df['date'] = pd.to_datetime(df['date'])
    df['low_price'] = pd.to_numeric(df['low_price'], errors='coerce')
    df['high_price'] = pd.to_numeric(df['high_price'], errors='coerce')

    mask = df['unit'] == '50.00-52.00'
    df.loc[mask, 'unit'] = 'প্রতি কেজি'
    
    unit_mapping = {
        'প্রতি কেজি': '১ কেজি',
        'প্রতি লিটার': '১ লিটার',
        'প্রতি কেজি প্যাঃ': '১ কেজি',
        'প্রতি মেঃটন': '১ মেঃটন',
        'প্রতি দিস্তা': '১ দিস্তা',
        'প্রতি হালি': '১ হালি',
    }
    df['unit'] = df['unit'].replace(unit_mapping)

    category_mapping = {
        'মসলাঃ': 'মসলা',
        'বিবিধঃ': 'বিবিধ', 
        'মাছ ও গোশত:': 'মাছ/গোশত',
    }
    df['category'] = df['category'].replace(category_mapping)

    item_mapping = {
        'চাল (মাঝারী)পাইজাম/আটাশ': 'চাল (মাঝারী)পাইজাম/লতা',
        'রসুন(দেশী)': 'রসুন (দেশী)',
        'রসুন(দেশী) নতুন/পুরাতন)': 'রসুন (দেশী)',
        'রসুন(দেশী) নতুন': 'রসুন (দেশী)',
        'রসুন (দেশী) নতুন/পুরাতন': 'রসুন (দেশী)',
        'রসুন(দেশী) নতুন/পুরাতন': 'রসুন (দেশী)',
        'রসুন(দেশী) পুরাতন': 'রসুন (দেশী)',
        'আদা (দেশী)(নতুন)': 'আদা (দেশী)',
        'আদা (দেশী) নতুন': 'আদা (দেশী)',
        'পিঁয়াজ (নতুন) (দেশী)': 'পিঁয়াজ (দেশী)',
        'পিঁয়াজ (নতুন/পুরাতন) (দেশী)': 'পিঁয়াজ (দেশী)',
        'আলু (নতুন, মানভেদে)': 'আলু (মানভেদে)',
        'আলু (নতুন/পুরাতন)(মানভেদে)': 'আলু (মানভেদে)',
        'মশুর ডাল (মাঝারী দানা)': 'মশূর ডাল (মাঝারী দানা)',
        'লবণ(প্যাঃ)আয়োডিনযুক্ত': 'লবণ(প্যাঃ)আয়োডিনযুক্ত(মানভেদে)',
        'সুপার পাম অয়েল (লুজ)': 'পাম অয়েল (সুপার)',
    }
    df['item'] = df['item'].replace(item_mapping)

    remove_conditions = (
        (df['item'] == 'পাম অলিন (বোতল)') |
        ((df['item'] == 'রাইস ব্রান তেল (বোতল)')) |
        ((df['item'] == 'সয়াবিন তেল (বোতল)') & (df['unit'] == '2 লিটার'))
    )
    
    df = df[~remove_conditions]
    
    df = df.sort_values(['item', 'unit', 'date'])
    df['low_price'] = df.groupby(['item', 'unit'])['low_price'].ffill()
    df['high_price'] = df.groupby(['item', 'unit'])['high_price'].ffill()
    
    return df


