package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

func getUrl(c *gin.Context, urlVarName string) {
	tinyUrl := c.Params.ByName(urlVarName)
	longUrl, err := GetLongUrl(tinyUrl)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"response": "Something went wrong with database: " + err.Error()})
		return
	}

	c.Redirect(302, longUrl)
}

func SetupWorker() *gin.Engine {
	r := gin.Default()

	urlVarName := "url"
	r.GET(fmt.Sprintf("/:%s", urlVarName), func(c *gin.Context) {
		getUrl(c, urlVarName)
	})

	return r
}
