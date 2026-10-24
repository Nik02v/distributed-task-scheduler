#include "protocol.h"

#include <stdexcept>
#include <string>

void send_message(const Socket& socket, const json& message)
{
    if (!message.is_object())
        throw std::invalid_argument("Message must be a JSON object");
    std::string body = message.dump();
    if (body.size() > max_message)
        throw std::invalid_argument("Message exceeds 1 MiB");
    std::uint32_t length = htonl(static_cast<std::uint32_t>(body.size()));
    send_all(socket, reinterpret_cast<const char*>(&length), sizeof(length));
    send_all(socket, body.data(), body.size());
}

bool receive_message(const Socket& socket, json& message)
{
    std::uint32_t length;
    if (!receive_all(socket, reinterpret_cast<char*>(&length), sizeof(length)))
        return false;
    length = ntohl(length);
    if (length == 0 || length > max_message)
        throw std::invalid_argument("Invalid TCP message length");
    std::string body(length, '\0');
    if (!receive_all(socket, body.data(), body.size()))
        throw std::runtime_error("Missing TCP message body");
    message = json::parse(body);
    if (!message.is_object())
        throw std::invalid_argument("Message is not a JSON object");
    return true;
}
