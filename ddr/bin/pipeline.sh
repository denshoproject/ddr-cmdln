# ----------------------------------------------------------------------
# 
EXAMPLE="EXAMPLE:  $ publish.sh gjost gjost@densho.org 192.168.56.1:9200 ddrpublic-dev /var/www/media/ddr/batch ddr-densho-122"


USER=$1
MAIL=$2
HOSTS=$3
INDEX=$4
DDRBASE=$5
COLLECTION=$6

if [ -z "$1" ]; then
    echo "USER arg not set"; echo $EXAMPLE; exit 1
fi
if [ -z "$2" ]; then
    echo "MAIL arg not set"; echo $EXAMPLE; exit 1
fi
if [ -z "$3" ]; then
    echo "ES_HOSTS arg not set"; echo $EXAMPLE; exit 1
fi
if [ -z "$4" ]; then
    echo "ES_INDEX arg not set"; echo $EXAMPLE; exit 1
fi
if [ -z "$5" ]; then
    echo "DDRBASE arg not set"; echo $EXAMPLE; exit 1
fi
if [ -z "$6" ]; then
    echo "COLLECTION arg not set"; echo "$EXAMPLE"; exit 1
fi


echo "========================================================================"

echo "Cloning $COLLECTION"
ddr clone --user=$USER --mail=$MAIL --cid $COLLECTION --dest=$DDRBASE/$COLLECTION
cd $DDRBASE/$COLLECTION
echo ""

echo "Removing origin remote"
git remote remove origin
echo ""

ddr-transform --user=$USER --mail=$MAIL $DDRBASE/$COLLECTION
cd $DDRBASE/$COLLECTION
git commit --all --message="ddr-transform"
echo ""
 
# use --keeptmp because 'ddr' cannot sudo
ddr-filter --keeptmp --destdir=$DDRBASE $DDRBASE/$COLLECTION
sh $DDRBASE/FILTER_$COLLECTION.sh | tee -a $DDRBASE/FILTER_$COLLECTION.log
rm -Rf $DDRBASE/FILTER_$COLLECTION
rm $DDRBASE/FILTER_$COLLECTION.sh
date;ls -l $DDRBASE
echo ""
 
ddr-signatures --user=$USER --mail=$MAIL $DDRBASE/PUBLIC_$COLLECTION
cd $DDRBASE/PUBLIC_$COLLECTION
git commit --all --message= "ddr-signature"
echo ""
 
ddr-index index --host=$HOSTS --index=$INDEX  $DDRBASE/PUBLIC_$COLLECTION
