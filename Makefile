server_image:
	docker build -f Dockerfile_server -t server_image .

lb_image:
	docker build -f Dockerfile_lb -t lb_image .

client_image:
	docker build -f Dockerfile_client -t client_image .

run_lb:
	docker run --privileged -dit --name load_balancer --hostname load_balancer -v /var/run/docker.sock:/var/run/docker.sock --network my_network -e "NUM_SERVER=$(N)" lb_image 

run_client:
	docker run -dit --name client --hostname client --network my_network client_image

run_server:
	docker run -dit --name server --hostname server --network my_network server_image

rm_server:
	docker rm -f server

rm_server_image: rm_server
	docker rmi server_image

rm_all:
	docker rm -f server1 server2 
	docker cp load_balancer.py 0e2:usr/src/app/