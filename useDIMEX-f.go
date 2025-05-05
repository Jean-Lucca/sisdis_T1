package main

import (
	"SD/DIMEX"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run useDIMEX-f.go 0 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX-f.go 1 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX-f.go 2 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		return
	}

	id, _ := strconv.Atoi(os.Args[1])
	addresses := os.Args[2:]
	// fmt.Print("id: ", id, "   ") fmt.Println(addresses)

	var dmx *DIMEX.DIMEX_Module = DIMEX.NewDIMEX(addresses, id, true)
	fmt.Println(dmx)

	// abre arquivo que TODOS processos devem poder usar
	file, err := os.OpenFile("./mxOUT.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close() // Ensure the file is closed at the end of the function

	// Espera para facilitar a inicialização de todos os processos
	time.Sleep(3 * time.Second)

	// ROtina para tirar snapshots periodicos

	go func() {
		for {
			fmt.Println("[ APP id:", id, " SNAPSHOT ]")
			dmx.Req <- DIMEX.SNAPSHOT
			time.Sleep(10000)
		}
	}()

	for {
		// SOLICITA ACESSO AO DIMEX
		//fmt.Println("[ APP id: ", id, " PEDE   MX ]")
		dmx.Req <- DIMEX.ENTER
		//fmt.Println("[ APP id: ", id, " ESPERA MX ]")
		// ESPERA LIBERACAO DO MODULO DIMEX
		<-dmx.Ind

		// A PARTIR DAQUI ESTA ACESSANDO O ARQUIVO SOZINHO
		_, err = file.WriteString("|") // Marca entrada no arquivo
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		fmt.Println("[ APP id: ", id, " *EM*   MX ]")

		// Espera
		time.Sleep(1000 * time.Millisecond)

		_, err = file.WriteString(".") // marca saida no arquivo
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		// AGORA VAI LIBERAR O ARQUIVO PARA OUTROS
		dmx.Req <- DIMEX.EXIT
		fmt.Println("[ APP id: ", id, " FORA   MX ]")

		// Espera
		time.Sleep(500 * time.Millisecond)
	}
}
