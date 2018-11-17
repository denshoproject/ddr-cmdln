# Add symlinks after installing (FPM) .deb package

touch /etc/ddr/ddrlocal-local.cfg
chown ddr.root /etc/ddr/ddrlocal-local.cfg
chmod 640 /etc/ddr/ddrlocal-local.cfg

# logs dir perms
mkdir -p /var/log/ddr
chmod 755 /var/log/ddr
chown -R ddr.ddr /var/log/ddr

# Install customized ImageMagick-6/policy.xml.  This disables default
# memory and cache limits put in place to protect against DDoS attacks
# but these are not an issue in our local install.
echo "Installing custom Imagemagick policy.xml"
cp /opt/ddr-local/ddr-cmdln/conf/imagemagick-policy.xml /etc/ImageMagick-6/policy.xml
