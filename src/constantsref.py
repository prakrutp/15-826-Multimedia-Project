DBNAME = 'prakrutp'
USERNAME = 'prakrutp'
PGHOST = '/tmp'
PGPORT = 15791
## Please provide full path
OUTPUT_LOCATION_BLOCKS = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia/15-826-Multimedia-Project/output/darpa_full_date.csv'
OUTPUT_LOCATION = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia/15-826-Multimedia-Project/output/darpa_full_date.txt'
INPUT_CSV = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia/15-826-Multimedia-Project/data/darpa_full_date.csv'
RAW_INPUT = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia/15-826-Multimedia-Project/data/darpa.csv'
OUTPUT_TABLE_NAME = 'k_dense_blocks'
BINARIZE = False
GRANULARITY = "date"

#### Input and output parameters
INPUT_DELIMITER = ','
## The fourth attribute is considered as count
NUM_ATTRIBUTES_TEMP = 5
NUM_ATTRIBUTES = 3
## Number of dense blocks to be output
NUM_DENSE_BLOCKS = 5
## One among 'A', 'G', 'S'
DENSITY_MEASURE = 'G'
## One among 'C', 'D'
POLICY = 'C'
