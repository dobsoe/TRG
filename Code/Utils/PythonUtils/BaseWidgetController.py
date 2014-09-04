# Base class for python widgets
import Utils.PythonUtils.TRGPythonFunctions
from IPython.html import widgets
from IPython.html.widgets import interact, interactive, fixed
from IPython.display import clear_output, display, HTML

class BaseWidgetController(): 

    def __init__(self):

        #initialise values
        self.Table=""
        self.Label=""
        self.Category=""
        self.SubCategory=""
        self.Retailer=""
        self.Brand=""
        self.MinPrice=""
        self.MaxPrice=""
        self.AnalysisType=""
        self.Country=""
        self.SearchTerm=""
        
        #initialise widgets
        self.CategoryWidget=widgets.DropdownWidget()
        self.SubCategoryWidget=widgets.DropdownWidget()
        self.BrandWidget=widgets.DropdownWidget()
        self.RetailerWidget=widgets.DropdownWidget()
        self.MinPriceWidget=widgets.FloatSliderWidget()
        self.MaxPriceWidget=widgets.FloatSliderWidget()
        self.AnalysisWidget=widgets.RadioButtonsWidget()
        self.DependenceWidget=widgets.RadioButtonsWidget()
        
        # set up connection
        self.fe=Utils.PythonUtils.TRGPythonFunctions.FrontEnd()
        self.fe.ConnectToDCA()
        
        # set up UI
        self.SetupUI()

    def GetCategories(self): 
        Categories=self.fe.executeandfetch("select category from " + self.Table + \
                                           " where country = '" + self.Country + \
                                           "' group by 1 order by 1");
        Categories=self.PrepareList(Categories, ["Choose a category"])
        self.CategoryWidget = widgets.DropdownWidget(values=Categories, description = "Choose a category:", sync=True)
        display(self.CategoryWidget)

    def GetSubCategories(self):
        SubCategories=self.fe.executeandfetch("select subcategory from " + self.Table + \
                                              " where country = '" + self.Country + \
                                              "' and category = '" + self.Category + \
                                              "' group by 1 order by 1");
        SubCategoryString=""
        for i in range(0,len(SubCategories)):
            SubCategoryString=SubCategoryString+SubCategories[i][0]+"|"
        SubCategories = list(set(SubCategoryString.split("|")))
        SubCategories  = filter(None, SubCategories)
        SubCategories.insert(0, "All")
        SubCategories.insert(0, "Choose a subcategory")
        self.SubCategoryWidget = widgets.DropdownWidget(values=SubCategories, description = "Choose a subcategory:", sync=True)
        display(self.SubCategoryWidget)
            
    def GetBrands(self):
        Brands=self.fe.executeandfetch("select facetbrand from " + self.Table + \
                                       " where category = '" + self.Category + \
                                       "' and country = '" + self.Country + \
                                       "' group by 1 order by count(1)");
        Brands=self.PrepareList(Brands, ["All", "Choose a brand"])
        self.BrandWidget = widgets.DropdownWidget(values=Brands, description = "Choose a brand:")
        display(self.BrandWidget)         

    def GetRetailers(self):
        ExecStatement="select retailername from " + self.Table + \
                                          " where category = '" + self.Category + \
                                          "' and subcategory like '%" + self.SubCategory + \
                                          "%' and facetbrand like '%" + self.Brand + \
                                          "%' and country = '" + self.Country + \
                                          "' group by 1 order by 1"
        Retailers=self.fe.executeandfetch(ExecStatement);
        Retailers=self.PrepareList(Retailers, ["All", "Choose a retailer"])
        self.RetailerWidget = widgets.DropdownWidget(values=Retailers, description = "Choose a retailer:")
        display(self.RetailerWidget) 

    def GetPriceBucket(self):
        self.UpdateValues()
        PriceLimits=self.fe.executeandfetch("select max(price_currentdaymode), min(price_currentdaymode) from " + self.Table + \
                                          " where category = '" + self.Category + \
                                          "' and subcategory like '%" + self.SubCategory + \
                                          "%' and facetbrand like '%" + self.Brand + \
                                          "%' and country = '" + self.Country + \
                                          "' and retailername like '%" + self.Retailer + "%'"); 

        # set price widget
        self.MinPriceWidget = widgets.FloatSliderWidget(max=PriceLimits[0][0], min=PriceLimits[0][1], value = PriceLimits[0][1], description="Min price")
        self.MaxPriceWidget = widgets.FloatSliderWidget(max=PriceLimits[0][0], min=PriceLimits[0][1], value = PriceLimits[0][0], description="Max price")
        display(self.MinPriceWidget) 
        display(self.MaxPriceWidget) 

    def CreateGo(self):
        self.UpdateValues()
        self.GoButton = widgets.ButtonWidget(description="Go")
        self.GoButton.on_click(self.GetVals)
        display(self.GoButton)

    def GetVals(self, value):
        self.UpdateValues()
        
        BatchStatement = "select " + self.SearchTerm + \
                         ", count(*) from " + self.Table + \
                         " where category = '" + self.Category + \
                         "' and subcategory like '%" + self.SubCategory + \
                         "%' and facetbrand like '%" + self.Brand + \
                         "%' and retailername like '%" + self.Retailer + \
                         "%' and price_currentdaymode > " + self.MinPrice + \
                         " and price_currentdaymode < " + self.MaxPrice + \
                         " and country = '" + self.Country + \
                         "' group by 1 order by 2"
        print BatchStatement
        self.plotvals=self.fe.executeandfetch(BatchStatement)
        self.plotvals=zip(*self.plotvals)
        self.MakePlot()
        
    def PrepareList(self, List, ExtraArgs):
        for i in range(0,len(List)):
            text = str(List[i][0])
            text="".join(i for i in text if ord(i)<128)
            List[i]=text
        List = filter(None, List)
        for i in ExtraArgs:
            List.insert(0, i)
        return List
    
    def UpdateValues(self):
        self.AnalysisType=self.AnalysisWidget.value   
        self.Country = str(self.CountryWidget.value)
        self.Category=str(self.CategoryWidget.value)
        self.SubCategory=str(self.SubCategoryWidget.value)
        self.Retailer=str(self.RetailerWidget.value)
        
        if(self.SubCategory=="All"):
            self.SubCategory=str("")
        if(self.Retailer=="All"):
            self.Retailer=str("")
        if(self.Brand=="All"):
            self.Brand=str("")
