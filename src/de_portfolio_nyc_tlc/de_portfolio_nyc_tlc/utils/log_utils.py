def log_w_header(message: str, header_char="#"):
    len_border = len(message) + 4
    border = header_char * len_border

    # add top border
    # add "# {message} #" and center message with :^ and x-padding of 1 space
    # add bottom border
    print(
        f"{border}\n{header_char}{message:^{len(message) + 2}}{header_char}\n{border}"
    )
