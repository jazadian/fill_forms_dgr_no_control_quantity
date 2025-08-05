# Start with the base image
FROM public.ecr.aws/lambda/python:3.9 AS build

# Install system-level dependencies for Chrome and FFmpeg
RUN yum install -y unzip atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm git

# Download and install Chrome and ChromeDriver
# Download ChromeDriver and Chrome
RUN curl -Lo "/tmp/chromedriver-linux64.zip" "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/119.0.6045.105/linux64/chromedriver-linux64.zip" && \
    curl -Lo "/tmp/chrome-linux64.zip" "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/119.0.6045.105/linux64/chrome-linux64.zip"

# Unzip ChromeDriver and move it to the /opt/ directory
RUN unzip /tmp/chromedriver-linux64.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /opt/chromedriver && \
    chmod +x /opt/chromedriver

# Unzip Chrome and move it to the /opt/ directory
# Unzip Chrome and move it to the /opt/chrome/ directory
RUN mkdir -p /opt/chrome && \
    unzip /tmp/chrome-linux64.zip -d /tmp/ && \
    mv /tmp/chrome-linux64/* /opt/chrome/ && \
    chmod +x /opt/chrome/chrome


# Install Python packages
RUN pip install --upgrade pip
RUN pip install --no-cache-dir setuptools-rust
RUN pip install --no-cache-dir selenium webdriver_manager fake_useragent pymongo

# Copy application code
COPY lambda_function.py fill_form_AM.py fill_form_I.py fill_form_Ampliacion.py fill_form_PF.py fill_form_PJ.py human_functions.py login.py submit.py notify_error.py ./

# Set the command to run the application
CMD ["lambda_function.lambda_handler"]


