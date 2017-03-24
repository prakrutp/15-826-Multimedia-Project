#### DB parameters - prakrutp
DBNAME = 'prakrutp'
USERNAME = 'prakrutp'
PGHOST = '/tmp'
PGPORT = 15790
## Please provide full path
OUTPUT_LOCATION_BLOCKS = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia_project/15-826-Multimedia-Project/output/test5/test5_DS.csv'
OUTPUT_LOCATION = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia_project/15-826-Multimedia-Project/output/test5/test5_DS.txt'
INPUT_CSV = '/afs/andrew.cmu.edu/usr19/prakrutp/private/multimedia_project/15-826-Multimedia-Project/data/test5.csv'
OUTPUT_TABLE_NAME = 'k_dense_blocks'

#### DB parameters - marora
"""
DBNAME = 'test'
USERNAME = 'marora'
PGHOST = '/tmp'
PGPORT = 15754
## Please provide full path
OUTPUT_LOCATION_BLOCKS = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/output/darpa_full_date_AD.csv'
OUTPUT_LOCATION = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/output/darpa_full_date_AD.txt'
INPUT_CSV = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/data/darpa_full_date.csv'
OUTPUT_TABLE_NAME = 'k_dense_blocks'
# OUTPUT_LOCATION_BLOCKS = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/output/test.csv'
# OUTPUT_LOCATION = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/output/test.txt'
# INPUT_CSV = '/afs/andrew.cmu.edu/usr13/marora/private/15826/15-826-Multimedia-Project/data/test.csv'
# OUTPUT_TABLE_NAME = '_k_dense_blocks'
"""
#### Input and output parameters
INPUT_DELIMITER = ','
## The fourth attribute is considered as count
NUM_ATTRIBUTES_TEMP = 5
NUM_ATTRIBUTES = 2
## Number of dense blocks to be output
NUM_DENSE_BLOCKS = 50
## One among 'A', 'G', 'S'
DENSITY_MEASURE = 'S'
## One among 'C', 'D'
POLICY = 'D'
