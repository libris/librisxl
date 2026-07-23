SECRET=$1

if [ "$SECRET" != '' ]; then
	java -Xmx2G -Dxl.secret.properties=$SECRET -jar build/libs/sru.jar
else
	echo Nope.
fi
