# Add symlinks after installing (FPM) .deb package

touch /etc/ddr/ddrlocal-local.cfg
chown ddr:root /etc/ddr/ddrlocal-local.cfg
chmod 640 /etc/ddr/ddrlocal-local.cfg

# logs dir perms
mkdir -p /var/log/ddr
chmod 755 /var/log/ddr
chown -R ddr:ddr /var/log/ddr

# Install customized ImageMagick-6/policy.xml.  This disables default
# memory and cache limits put in place to protect against DDoS attacks
# but these are not an issue in our local install.
echo "Installing custom Imagemagick policy.xml"
# Release name e.g. jessie
DEBIAN_CODENAME=$(lsb_release -sc)
if [ $DEBIAN_CODENAME = 'bullseye' ]
then
    cp /opt/ddr-cmdln/conf/imagemagick-policy.xml.deb11 /etc/ImageMagick-6/policy.xml
fi
if [ $DEBIAN_CODENAME = 'bookworm' ]
then
    cp /opt/ddr-cmdln/conf/imagemagick-policy.xml.deb12 /etc/ImageMagick-6/policy.xml
fi
