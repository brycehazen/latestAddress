import os
import pandas as pd
import math
from multiprocessing import Process
from fuzzywuzzy import fuzz
import re

def combine_csv_files():
    files = os.listdir(os.getcwd())
    csv_files = [f for f in files if 'processed_chunk_' in f]
    combined_dataframes = [pd.read_csv(f, dtype=str, low_memory=False) for f in csv_files]
    combined_df = pd.concat(combined_dataframes, ignore_index=True)
    column_order = ['ConsID', 'Normalized_CnAdrPrf_Addrline1', 'CnAdrPrf_DateLastChanged','CnAdrPrf_DateAdded', 'AddressDates', 
                    'CnAdrPrf_Info_Source','StreetChanged', 'Normalized_PrefAddrLines','mMatch', 'ConfidenceScore','Parish']
    combined_df = combined_df[column_order + [col for col in combined_df.columns if col not in column_order]]
    combined_df.to_csv('fuzzycomparev2.csv', index=False)

def get_street_suffix_mapping():
    street_suffix_mapping = {
        'ALLEY': 'ALY',
        'ALLEE': 'ALY',
        'ANEX': 'ANX',
         'ANNEX': 'ANX',
        'ARCADE': 'ARC',
        'ARC': 'ARC',
        'AVENUE': 'AVE',
        'AV': 'AVE',
        'BAYOU': 'BYU',
        'BAYOO': 'BYU',
        'BEACH': 'BCH',
        'BCH': 'BCH',
        'BEND': 'BND',
        'BLUFF': 'BLF',
        'BLF': 'BLF',
        'BLUFFS': 'BLFS',
        'BOTTOM': 'BTM',
        'BOT': 'BTM',
        'BOULEVARD': 'BLVD',
        'BLVD': 'BLVD',
        'BRANCH': 'BR',
        'BR': 'BR',
        'BRIDGE': 'BRG',
        'BRDGE': 'BRG',
        'BROOK': 'BRK',
        'BRK': 'BRK',
        'BROOKS': 'BRKS',
        'BURG': 'BG',
        'BURGS': 'BGS',
        'BYPASS': 'BYP',
        'BYP': 'BYP',
        'CAMP': 'CP',
        'CANYON': 'CYN',
        'CANYN': 'CYN',
        'CAPE': 'CPE',
        'CAUSEWAY': 'CSWY',
        'CENTER': 'CTR',
        'CEN': 'CTR',
        'CENTERS': 'CTRS',
        'CIRCLE': 'CIR',
        'CIR': 'CIR',
        'CIRCLES': 'CIRS',
        'CLIFF': 'CLF',
        'CLF': 'CLF',
        'CLIFFS': 'CLFS',
        'CLFS': 'CLFS',
        'CLUB': 'CLB',
        'CLB': 'CLB',
        'COMMON': 'CMN',
        'COMMONS': 'CMNS',
        'CORNER': 'COR',
        'CONCOURSE': 'CONC',
        'COR': 'COR',
        'CORNERS': 'CORS',
        'COURSE': 'CRSE',
        'COURT': 'CT',
        'COURTS': 'CTS',
        'COVE': 'CV',
        'COVES': 'CVS',
        'CREEK': 'CRK',
        'CRESCENT': 'CRES',
        'CREST': 'CRST',
        'CROSSING': 'XING',
        'CROSSROAD': 'XRD',
        'CROSSROADS': 'XRDS',
        'CURVE': 'CURV',
        'DALE': 'DL',
        'DAM': 'DM',
        'DIVIDE': 'DV',
        'DIV': 'DV',
        'DRIVE': 'DR',
        'DR': 'DR',
        'DRIVES': 'DRS',
        'ESTATE': 'EST',
        'EST': 'EST',
        'ESTATES': 'ESTS',
        'EXPRESSWAY': 'EXPY',
        'EXP': 'EXPY',
        'EXTENSION': 'EXT',
        'EXT': 'EXT',
        'EXTENSIONS': 'EXTS',
        'EXTS': 'EXTS',
        'FALL': 'FALL',
        'FALLS': 'FLS',
        'FERRY': 'FRY',
        'FIELD': 'FLD',
        'FIELDS': 'FLDS',
        'FLAT': 'FLT',
        'FLATS': 'FLTS',
        'FORD': 'FRD',
        'FORDS': 'FRDS',
        'FOREST': 'FRST',
        'FORGE': 'FRG',
        'FORG': 'FRG',
        'FORGES': 'FRGS',
        'FORK': 'FRK',
        'FORKS': 'FRKS',
        'FORT': 'FT',
        'FREEWAY': 'FWY',
        'GARDEN': 'GDN',
        'GARDENS': 'GDNS',
        'GATEWAY': 'GTWY',
        'GLEN': 'GLN',
        'GLENS': 'GLNS',
        'GREEN': 'GRN',
        'GREENS': 'GRNS',
        'GROVE': 'GRV',
        'GROV': 'GRV',
        'GROVES': 'GRVS',
        'HARBOR': 'HBR',
        'HARB': 'HBR',
        'HARBORS': 'HBRS',
        'HAVEN': 'HVN',
        'HEIGHTS': 'HTS',
        'HT': 'HTS',
        'HIGHWAY': 'HWY',
        'HILL': 'HL',
        'HILLS': 'HLS',
        'HOLLOW': 'HOLW',
        'HLLW': 'HOLW',
        'INLET': 'INLT',
        'INLT': 'INLT',
        'ISLAND': 'IS',
        'IS': 'IS',
        'ISLANDS': 'ISS',
        'ISLE': 'ISLE',
        'JUNCTION': 'JCT',
        'JCT': 'JCT',
        'JUNCTIONS': 'JCTS',
        'JCTNS': 'JCTS',
        'KEY': 'KY',
        'KEYS': 'KYS',
        'KNOLL': 'KNL',
        'KNL': 'KNL',
        'KNOLLS': 'KNLS',
        'KNLS': 'KNLS',
        'LAKE': 'LK',
        'LK': 'LK',
        'LAKES': 'LKS',
        'LKS': 'LKS',
        'LAND': 'LAND',
        'LANDING': 'LNDG',
        'LANE': 'LN',
        'LIGHT': 'LGT',
        'LGT': 'LGT',
        'LIGHTS': 'LGTS',
        'LOAF': 'LF',
        'LF': 'LF',
        'LOCK': 'LCK',
        'LCK': 'LCK',
        'LOCKS': 'LCKS',
        'LCKS': 'LCKS',
        'LODGE': 'LDG',
        'LDG': 'LDG',
        'LOOP': 'LOOP',
        'MALL': 'MALL',
        'MANOR': 'MNR',
        'MNR': 'MNR',
        'MANORS': 'MNRS',
        'MEADOW': 'MDW',
        'MEADOWS': 'MDWS',
        'MDW': 'MDWS',
        'MEWS': 'MEWS',
        'MILL': 'ML',
        'MILLS': 'MLS',
        'MISSION': 'MSN',
        'MISSN': 'MSN',
        'MOTORWAY': 'MTWY',
        'MOUNT': 'MT',
        'MNT': 'MT',
        'MOUNTAIN': 'MTN',
        'MNTAIN': 'MTN',
        'MOUNTAINS': 'MTNS',
        'MNTNS': 'MTNS',
        'NECK': 'NCK',
        'NCK': 'NCK',
        'ORCHARD': 'ORCH',
        'ORCH': 'ORCH',
        'OVAL': 'OVAL',
        'OVERPASS': 'OPAS',
        'PARK': 'PARK',
        'PARKS': 'PARK',
        'PARKWAY': 'PKWY',
        'PARKWAYS': 'PKWY',
        'PASS': 'PASS',
        'PASSAGE': 'PSGE',
        'PATH': 'PATH',
        'PIKE': 'PIKE',
        'PINE': 'PNE',
        'PINES': 'PNES',
        'PLACE': 'PL',
        'PL': 'PL',
        'PLAIN': 'PLN',
        'PLAINS': 'PLNS',
        'PLAZA': 'PLZ',
        'POINT': 'PT',
        'POINTS': 'PTS',
        'PORT': 'PRT',
        'PORTS': 'PRTS',
        'PRAIRIE': 'PR',
        'PR': 'PR',
        'RADIAL': 'RADL',
        'RAD': 'RADL',
        'RAMP': 'RAMP',
        'RANCH': 'RNCH',
        'RAPID': 'RPD',
        'RAPIDS': 'RPDS',
        'REST': 'RST',
        'RIDGE': 'RDG',
        'RDG': 'RDG',
        'RIDGES': 'RDGS',
        'RDGS': 'RDGS',
        'RIVER': 'RIV',
        'RIV': 'RIV',
        'ROAD': 'RD',
        'RD': 'RD',
        'ROADS': 'RDS',
        'ROUTE': 'RTE',
        'ROW': 'ROW',
        'RUE': 'RUE',
        'RUN': 'RUN',
        'SHOAL': 'SHL',
        'SHL': 'SHL',
        'SHOALS': 'SHLS',
        'SHLS': 'SHLS',
        'SHORE': 'SHR',
        'SHOAR': 'SHR',
        'SHORES': 'SHRS',
        'SHOARS': 'SHRS',
        'SKYWAY': 'SKWY',
        'SPRING': 'SPG',
        'SPG': 'SPG',
        'SPRINGS': 'SPGS',
        'SPGS': 'SPGS',
        'SPUR': 'SPUR',
        'SPURS': 'SPUR',
        'SQUARE': 'SQ',
        'SQ': 'SQ',
        'SQUARES': 'SQS',
        'SQRS': 'SQS',
        'STATION': 'STA',
        'STA': 'STA',
        'STRAVENUE': 'STRA',
        'STRA': 'STRA',
        'STREAM': 'STRM',
        'STREET': 'ST',
        'STREETS': 'STS',
        'SUMMIT': 'SMT',
        'SMT': 'SMT',
        'TERRACE': 'TER',
        'TER': 'TER',
        'THROUGHWAY': 'TRWY',
        'TRACE': 'TRCE',
        'TRACK': 'TRAK',
        'TRAFFICWAY': 'TRFY',
        'TRAIL': 'TRL',
        'TRAILER': 'TRLR',
        'TUNNEL': 'TUNL',
        'TUNEL': 'TUNL',
        'TURNPIKE': 'TPKE',
        'TRNPK': 'TPKE',
        'UNDERPASS': 'UPAS',
        'UNION': 'UN',
        'UN': 'UN',
        'UNIONS': 'UNS',
        'VALLEY': 'VLY',
        'VALLEYS': 'VLYS',
        'VIADUCT': 'VIA',
        'VDCT': 'VIA',
        'VIEW': 'VW',
        'VIEWS': 'VWS',
        'VILLAGE': 'VLG',
        'VILL': 'VLG',
        'VILLAGES': 'VLGS',
        'VILLE': 'VL',
        'VISTA': 'VIS',
        'VIS': 'VIS',
        'WALK': 'WALK',
        'WALKS': 'WALK',
        'WALL': 'WALL',
        'WAY': 'WAY',
        'WY': 'WAY',
        'WAYS': 'WAYS',
        'WELL': 'WL',
        'WELLS': 'WLS',
        'NORTH': 'N',
        'EAST': 'E',
        'SOUTH': 'S',
        'WEST': 'W',
        'NORTHEAST': 'NE',
        'SOUTHEAST': 'SE',
        'NORTHWEST': 'NW',
        'SOUTHWEST': 'SW',
    }
    return street_suffix_mapping

def address_normalization(address):
    if not isinstance(address, str):
        return ''
    address = re.sub(r'[^\w\s]', '', address.replace('\\n', ' ')).upper()
    mapping = get_street_suffix_mapping()
    words = address.split()
    normalized_words = [mapping.get(word, word) for word in words]
    return ' '.join(normalized_words)

def process_fuzzy_compare(chunk, chunk_index):
    if 'PrefAddrLines' in chunk.columns and 'CnAdrPrf_Addrline1' in chunk.columns:
        chunk['Normalized_PrefAddrLines'] = chunk['PrefAddrLines'].apply(address_normalization)
        chunk['Normalized_CnAdrPrf_Addrline1'] = chunk['CnAdrPrf_Addrline1'].apply(address_normalization)
        results = chunk.apply(lambda x: compare_addresses(x['Normalized_PrefAddrLines'], x['Normalized_CnAdrPrf_Addrline1']), axis=1)
        chunk['mMatch'] = results.apply(lambda x: x[0])
        chunk['ConfidenceScore'] = results.apply(lambda x: x[1])
        chunk['AddressDates'] = chunk.apply(lambda row: 'Parish' if pd.to_datetime(row['StreetChanged']) > pd.to_datetime(row['CnAdrPrf_DateLastChanged']) else 'Cfocf', axis=1)
    else:
        chunk['mMatch'] = 'Column missing'
        chunk['ConfidenceScore'] = 0
        chunk['AddressDates'] = 'Data missing'
    filename = f'processed_chunk_{chunk_index}.csv'
    chunk.to_csv(filename, index=False)
    print(f'Chunk {chunk_index} processed and saved as {filename}')

def compare_addresses(addr1, addr2):
    if pd.notna(addr1) and pd.notna(addr2):
        score = fuzz.ratio(addr1, addr2)
        is_match = 'Match' if score >= 80 else 'No Match'
        return (is_match, score)
    else:
        return ('Missing Data', 0)

def split_and_process_dataframe(df):
    num_cpus = os.cpu_count()
    chunk_size = math.ceil(len(df) / num_cpus)
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    processes = []
    for i, chunk in enumerate(chunks):
        p = Process(target=process_fuzzy_compare, args=(chunk, i))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    combine_csv_files()

if __name__ == "__main__":
    try:
        df = pd.read_csv('final_combined.csv', encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv('final_combined.csv', encoding='ISO-8859-1', low_memory=False)
    split_and_process_dataframe(df)
