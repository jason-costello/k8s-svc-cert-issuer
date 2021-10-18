package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/gosuri/uilive"
	"github.com/sirupsen/logrus"
	"gopkg.in/ryankurte/go-async-cmd.v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
type Service struct{
	Name string `json:"name"csv:"name"`
	Port string`json:"port"csv:"port"`
	Context string`json:"context"csv:"context"`
	Namespace string`json:"namespace"csv:"namespace"`
	CertIssuer string`json:"cert_issuer"csv:"cert_issuer"`
	Error string`json:"error"csv:"error"`
	StatusCode int`json:"-"csv:"-"`
}

type Context struct{
	Name string
	Namespace string
	Status string
	StatusCode int
}


var issuerPattern=	regexp.MustCompile(`O=(?P<issuer>.*),`)


var ttlSvcs uint32
var doneSvcs int32

func main() {

	verbose := flag.Bool("verbose", false, "verbose output, defaults to false")
	jsonFmt := flag.Bool("json", false, "output results in json format")
	csvFmt := flag.Bool("csv", true, "output results in csv format")
	outFile := flag.String("o", "", "output filename")



	// get kube config file
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()




	var logger = logrus.New()

	if *verbose{
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	contextWriter := uilive.New()

	contextWriter.Start()
	fmt.Fprintf(contextWriter, "Gathering list of contexts\n")
	time.Sleep(5 * time.Millisecond)
	kubeContexts, err := getContexts()
	if err != nil{
		logger.Fatal("Failed to get context list: ", err.Error())
	}

	fmt.Fprintf(contextWriter, "%d contexts found\n", len(kubeContexts))


	// put contexts on channel
	gen := func(done <-chan interface{}, logger *logrus.Logger,  kubeconfig *string, kubeContext ...Context) <-chan Context {
		contextStream := make(chan Context)
		go func() {
			defer close(contextStream)

			for _, kctx := range kubeContext {


				if kctx.Name != "" && kctx.Namespace != "" {
					select {
					case <-done:
						return
					case contextStream <- kctx:
					}
				}

			}

		}()
		return contextStream
	} // end gen func



	// put services on a channel
	getServiceListStage := func(done <-chan interface{}, incomingStream <-chan Context) <-chan Service {
		serviceStream := make(chan Service)
		if err != nil{
			panic(err)
		}
		go func() {
			defer close(serviceStream)

			for  c := range incomingStream {

				if c.Name != "" && c.Namespace != "" {
					//logger.Infof("Grabbing service list for context %s ", c.Name)


					svcs, err := getServices(c.Name, c.Namespace, kubeconfig)
					if err != nil{
					c.Status = err.Error()
					c.StatusCode = 100
					<-done
					return
					}
					for _, s := range svcs {
						s.Error = ""
						s.StatusCode = 0
						s.Context = c.Name
						s.Namespace = c.Namespace
						atomic.AddUint32(&ttlSvcs, 1)
						select {
						case <-done:
							return
						case serviceStream <- s:
						}
					}
				}
			}
		}()
		return serviceStream
	}



	getCertIssuerStage := func( done <- chan interface{}, incomingStream <-chan Service) <- chan Service{
		serviceStream := make(chan Service)


		go func(){
			defer close(serviceStream)

			for s := range incomingStream{
				fmt.Fprintf(contextWriter,"%d of %d services processed\n", doneSvcs * -1, ttlSvcs)
				time.Sleep(5 * time.Millisecond)
				if s.StatusCode == 0 {

					ns := processService(s)
					s = ns
					atomic.AddInt32(&doneSvcs, -1)

				}


				select{
				case <-done:
					return
				case serviceStream <- s:
				}
			}

		}()



		return serviceStream
	}


	// fan-in implementation to consolidate the result channels
	fanIn := func(done <-chan interface{}, channels ...<-chan Service) <-chan Service {

		var wg sync.WaitGroup
		mplexStream := make(chan Service)

		mplex := func(c <-chan Service) {
			defer wg.Done()
			for i := range c {
				select {
				case <-done:
					return
				case mplexStream <- i:
				}
			}
		}
		wg.Add(len(channels))
		for _, c := range channels {
			go mplex(c)
		}

		go func() {
			wg.Wait()
			close(mplexStream)
		}()

		return mplexStream
	} // end fan-in



done := make(chan interface{})
defer close(done)
contextStream := gen(done, logger,  kubeconfig, kubeContexts...)

contextProcessors := make([]<-chan Service, len(kubeContexts))




for i := 0; i < len(contextProcessors); i++{

	contextProcessors[i] = getCertIssuerStage( done, getServiceListStage(done, contextStream))
}

type ServiceResults struct{
	Results []Service `json:"results"`
}

	// cs will hold the final consolidatiteritermed services from all channels
cs := ServiceResults{}

	// fanIn used to consolidate all the results to cs

	for x := range fanIn(done, contextProcessors...) {
		cs.Results = append(cs.Results, x)
	}

	var outputData []byte
	var outputDataStr string
	// marshal results and return

	var fileFmt string
	if *jsonFmt  && !*csvFmt{
		outputData, err = json.Marshal(cs)
		fileFmt = "json"
		if err != nil {
			logger.Error("Failed to marshal results: ", err)
			return
		}
		outputDataStr = string(outputData)
	} else {
		fileFmt = "csv"
		outputDataStr, err = gocsv.MarshalString(&cs.Results) // Get all clients as CSV string
		if err != nil{
			logger.Error("Failed to generate csv file: ", err)
			logger.Println(cs)
		}

	}


	if outFile != nil{
		fmt.Fprintf(contextWriter, "Writing output file %s in %s format\n", *outFile, fileFmt)

		if *outFile != ""{
			f, err := os.Create(*outFile)
			if err != nil{
				logger.Fatal("Unable to write file")
			}
			defer f.Close()

		n, err := fmt.Fprint(f, outputDataStr)
		if err != nil{
			logger.Fatal("Unable to write file: ", err.Error())
		}
			fmt.Fprintf(contextWriter, "%d bytes written to %s", n, *outFile)
		}
	} else {
		logger.Println(outputDataStr)
	}





	fmt.Fprintln(contextWriter, "Processing complete")
	contextWriter.Stop()
}





func processService(s Service) Service{
	if s.Name == "docker-desktop" {
		s.Error="docker-desktop service"
		s.StatusCode=200
		return s
	}

	cStr := fmt.Sprintf("--context=%s", s.Context)
	nsStr := fmt.Sprintf("--namespace=%s",s.Namespace)
	svcStr := fmt.Sprintf("svc/%s", s.Name)
	// port forward
	pfc, port := portForward( cStr, nsStr, svcStr)
	pfc.ShowOutput = false
	out, ok := <- pfc.OutputChan
	if strings.Contains(strings.ToLower(strings.TrimSpace(out)), "error"){
		pfc.Exit()
			s.StatusCode=210
			s.Error= fmt.Sprintf(out)
			return s

	}

	if !ok{
		pfc.Exit()
		s.StatusCode = 220
		s.Error = "output returned !OK"
	}

	//wait until the port is open before proceeding
	for !IsOpened("localhost", port){
		time.Sleep(1 * time.Second)
	}

	// get cert
	certInfo, err := getCertIssuer("localhost", port)
	if err != nil {
		pfc.Exit()
		s.Error = err.Error()
		s.StatusCode = 230
		return s

	}

	// match for issuer and return value
	r := reSubMatchMap(issuerPattern, certInfo)
	s.CertIssuer = r["issuer"]

	pfc.Exit()

	return s
	}



// portForward setups up the kubectl port-forward command and starts running it in the background
func portForward( cStr, nsStr, svcStr string) (*gocmd.Cmd, string){

		//// args := fmt.Sprintf("%s port-forward %s 8080:443", cStr, svcStr)
		//pfc := exec.CommandContext(ctx, "kubectl", cStr, "port-forward",svcStr, "8080:443")
		//// pfc.Stdout = os.Stdout
		//pfc.Stderr = os.Stderr
		//pfc.Start()
		var port string
		var err error
		if port, err =  getPort(); err != nil{
			return nil, port
		}
		ports := fmt.Sprintf("%s:%s", port, "443")

		c := gocmd.Command( "kubectl", cStr, "port-forward",svcStr, ports)
		c.OutputChan = make(chan string, 1024)
		c.ShowOutput = false
		err = c.Start()
		if err != nil{
			c.Exit()

			return nil, port
		}
		return c, port
}

// getCertIssuer returns a string containing the issuer org name from the cert that is on the provided host:port
func getCertIssuer( host string, port string) (string, error){

	conn, err := tls.Dial("tcp",fmt.Sprintf("%s:%s", host, port), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	state := conn.ConnectionState()

	if len(state.PeerCertificates) > 0{
      return state.PeerCertificates[0].Issuer.String(), nil
	}

	return "", nil


}




// getContexts returns a slice of Context structs
func getContexts() ([]Context, error) {
	pattern  := regexp.MustCompile(`^(?P<context>[\S]{3,})\s+(?P<cluster>[\S]{3,})\s+(?P<authInfo>[\S]{0,})\s*(?P<namespace>[\S]{0,})$`)

	var contexts []Context

	cmd := exec.Command("kubectl", "config", "get-contexts")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {

		return nil, err
	}

	lines := strings.Split(out.String(), "\n")
	for x, l := range lines{
		if x == 0{
			continue
		}

		l := strings.Replace(strings.TrimSpace(l), "*", "",-1)

		parts := reSubMatchMap(pattern, l)
		if parts == nil{
			continue
		}

		contexts = append(contexts, Context{
			Name:      parts["context"],
			Namespace: parts["namespace"],
		})
	}
	return contexts, nil
}

// reSubMatchMap builds a map of values from a regex match that has named parameters
func reSubMatchMap(r *regexp.Regexp, str string) map[string]string {
	match := r.FindStringSubmatch(str)
	if len(match) < 1{
		return nil
	}
	subMatchMap := make(map[string]string)
	for i, name := range r.SubexpNames() {
		if i != 0 {
			subMatchMap[name] = match[i]
		}
	}

	return subMatchMap
}


// getServices returns a slice  of service structs from the namespace and context
func getServices( kubeContext  , namespace string, kubeConfig *string) ([]Service, error){


	if kubeConfig == nil{
		return nil, errors.New("kube config is nil")
	}



	// use the current context in kubeconfig
	kConfig, err := buildConfigFromFlags(kubeContext, *kubeConfig)
	if err != nil {
		return nil, err
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(kConfig)
	if err != nil {
		return nil, err
	}



	svc, err := clientset.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var svcs []Service
	for _, s := range svc.Items{
		nsvc := Service{Name: s.Name, Port: "0"}

		if len(s.Spec.Ports) > 0{
				nsvc.Port = fmt.Sprint(s.Spec.Ports[0].Port)
		}



		svcs=append(svcs, nsvc)
	}

	return svcs, nil


}


// buildConfigFromFlags grabs the config for k8s api set to the contexts passed in
func buildConfigFromFlags(context, kubeconfigPath string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&clientcmd.ConfigOverrides{
			CurrentContext: context,
		}).ClientConfig()
}

func getPort()(string, error){
	l, err := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
if err != nil{
	return "", err
}
	return strings.Split(l.Addr().String(), ":")[1], nil

}

// IsOpened returns true if the host:port is open and false if not
func IsOpened(host string, port string) bool {
	timeout := 500 * time.Millisecond
	target := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return false
	}
	if conn != nil {
		conn.Close()
		return true
	}
	return false
}
