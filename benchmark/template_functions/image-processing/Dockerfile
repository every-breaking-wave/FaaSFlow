FROM codeless_base
COPY config.py /proxy
RUN mkdir /blocks
COPY blocks /blocks
COPY requirements.txt /proxy
COPY test_pic.png /proxy
RUN pip3 install -r /proxy/requirements.txt