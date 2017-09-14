# Add symlinks after installing (FPM) .deb package

touch /etc/ddr/ddrlocal-local.cfg
chown ddr.root /etc/ddr/ddrlocal-local.cfg
chmod 640 /etc/ddr/ddrlocal-local.cfg

# logs dir perms
mkdir -p /var/log/ddr
chmod 755 /var/log/ddr
chown -R ddr.ddr /var/log/ddr
