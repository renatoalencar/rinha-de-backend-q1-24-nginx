INTERCEPT_BUILD := intercept-build

default:	./nginx/objs/nginx

./nginx/Makefile:
	cd nginx; ${INTERCEPT_BUILD} ./auto/configure \
		--add-module=../ngx_rinha_de_backend_module \
		--with-debug

./nginx/objs/nginx:	./nginx/Makefile
	cd nginx; ${INTERCEPT_BUILD} make -j

start:	./nginx/objs/nginx
	cp ./nginx/compile_commands.json .
	./nginx/objs/nginx -p ./ -c ./nginx.conf