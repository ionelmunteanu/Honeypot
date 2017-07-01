
#docker rm -f $(docker ps -aq)

#docker rmi -f n1

rm -fr ./antidote
cp -r ../rel/antidote ./
rm -fr ./antidote/data
rm -fr ./antidote/logs

#docker build . -t n2
#docker run -it --name a2 n2