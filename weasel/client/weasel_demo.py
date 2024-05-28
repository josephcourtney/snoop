import uvicorn


def main():
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8081,
        reload=True,
    )


if __name__ == "__main__":
    main()
