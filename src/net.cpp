#include "net.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace {
int socket_error()
{
    return WSAGetLastError();
}

bool interrupted(int error)
{
    return error == WSAEINTR;
}

std::runtime_error net_error(const char* operation)
{
    return std::runtime_error(std::string(operation) + ": socket error " + std::to_string(socket_error()));
}

void send_timeout(const Socket& socket)
{
    DWORD timeout = 1000;
    if (setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO,
                   reinterpret_cast<const char*>(&timeout), sizeof(timeout)) != 0)
        throw net_error("SO_SNDTIMEO");
}
}

Network::Network()
{
    WSADATA data;
    int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0)
        throw std::runtime_error("WSAStartup: " + std::to_string(result));
}

Network::~Network()
{
    WSACleanup();
}

Socket::~Socket()
{
    close();
}

Socket::Socket(Socket&& other) noexcept : fd(other.fd), stopped(other.stopped.load())
{
    other.fd = invalid_socket;
}

Socket& Socket::operator=(Socket&& other) noexcept
{
    if (this != &other) {
        close();
        fd = other.fd;
        stopped.store(other.stopped.load());
        other.fd = invalid_socket;
    }
    return *this;
}

void Socket::close()
{
    if (fd == invalid_socket)
        return;
    closesocket(fd);
    fd = invalid_socket;
}

void Socket::shutdown_all() const
{
    stopped.store(true);
    if (fd == invalid_socket)
        return;
    ::shutdown(fd, SD_BOTH);
}

void Socket::shutdown_write() const
{
    if (fd == invalid_socket)
        return;
    ::shutdown(fd, SD_SEND);
}

Socket listen_local(int port)
{
    Socket listener(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (listener.get() == invalid_socket)
        throw net_error("socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<unsigned short>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        throw net_error("bind");
    if (::listen(listener.get(), 32) != 0)
        throw net_error("listen");
    return listener;
}

Socket connect_to(const std::string& host, int port)
{
    Socket socket(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (socket.get() == invalid_socket)
        throw net_error("socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<unsigned short>(port));
    if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1)
        throw std::invalid_argument("host must be an IPv4 address");
    if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        throw net_error("connect");
    send_timeout(socket);
    return socket;
}

Socket accept_one(const Socket& listener)
{
    SocketHandle fd;
    do {
        fd = ::accept(listener.get(), nullptr, nullptr);
    } while (fd == invalid_socket && interrupted(socket_error()));
    if (fd == invalid_socket)
        throw net_error("accept");
    Socket socket(fd);
    send_timeout(socket);
    return socket;
}

int local_port(const Socket& socket)
{
    sockaddr_in address{};
    int size = sizeof(address);
    if (getsockname(socket.get(), reinterpret_cast<sockaddr*>(&address), &size) != 0)
        throw net_error("getsockname");
    return ntohs(address.sin_port);
}

bool wait_readable(const Socket& socket, int timeout_ms)
{
    fd_set set;
    FD_ZERO(&set);
    FD_SET(socket.get(), &set);
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    int result = ::select(0, &set, nullptr, nullptr, &timeout);
    if (result < 0) {
        if (interrupted(socket_error()))
            return false;
        throw net_error("select");
    }
    return result > 0;
}

void send_all(const Socket& socket, const char* data, std::size_t size)
{
    std::size_t sent = 0;
    while (sent < size) {
        if (socket.is_shutdown())
            throw std::runtime_error("Socket is shut down");
        int count = static_cast<int>(std::min<std::size_t>(size - sent, 1024 * 1024));
        int n = ::send(socket.get(), data + sent, count, 0);
        if (n < 0 && interrupted(socket_error()))
            continue;
        if (n <= 0)
            throw net_error("send");
        sent += static_cast<std::size_t>(n);
    }
}

bool receive_all(const Socket& socket, char* data, std::size_t size)
{
    std::size_t received = 0;
    while (received < size) {
        if (socket.is_shutdown())
            throw std::runtime_error("Socket is shut down");

        if (!wait_readable(socket, 100))
            continue;
        if (socket.is_shutdown())
            throw std::runtime_error("Socket is shut down");
        int count = static_cast<int>(std::min<std::size_t>(size - received, 1024 * 1024));
        int n = ::recv(socket.get(), data + received, count, 0);
        if (n < 0 && interrupted(socket_error()))
            continue;
        if (n < 0)
            throw net_error("recv");
        if (n == 0) {
            if (received == 0)
                return false;
            throw std::runtime_error("Connection closed during an incomplete TCP message");
        }
        received += static_cast<std::size_t>(n);
    }
    return true;
}
