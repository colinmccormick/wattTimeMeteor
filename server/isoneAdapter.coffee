SERVICE_NAME = 'NewEngland'

url = 'http://isoexpress.iso-ne.com/ws/wsclient'
payload = {'_ns0_requestType':'url', '_ns0_requestUrl':'/genfuelmix/current'}

Meteor.startup ->
  Meteor.setInterval ->
    console.log "Retrieving data from", SERVICE_NAME
    response = HTTP.post url, params:payload
    # response.data: [ { data: [Object], namespace: '_ns0_' } ] 
    data = response.data[0].data
    ###
    data = { _url: '/genfuelmix/current',
      GenFuelMixes: 
      { GenFuelMix:  [ [Object], [Object], [Object], [Object],[Object], [Object], [Object], [Object] ] } }
    ###
    mixes = data.GenFuelMixes.GenFuelMix
    ###
    mix =  { FuelCategory: 'Coal',
    BeginDate: '2013-12-05T20:37:20.000-05:00', GenMw: 708, FuelCategoryRollup: 'Coal', MarginalFlag: 'N' }
    ###
    for mix in mixes
      datum =
        authority: SERVICE_NAME
        timestamp: mix.BeginDate
        amount: mix.GenMw
        fuelType: mix.FuelCategory
      GenerationInterval.upsert(datum, datum)
  , 60*1000

###
class NEParser(UtilityParser):
    def __init__(self, request_method = None):
        self.MODEL = NE
        if request_method is None:
            url = 'http://isoexpress.iso-ne.com/ws/wsclient'
            payload = {'_ns0_requestType':'url', '_ns0_requestUrl':'/genfuelmix/current'}
            def wrapper():
                return requests.post(url, data = payload).json()
            self.request_method = wrapper
        else:
            self.request_method = request_method
            
    def _fraction_clean(self, row):
        return (row.hydro + row.other_renewable) / float(row.gas + row.nuclear + row.hydro + row.coal + row.other_renewable + row.other_fossil)

    def _total_MW(self, row):
        return float(row.gas + row.nuclear + row.hydro + row.coal + row.other_renewable + row.other_fossil)
        
    def update(self):
        try:
            json = self.request_method()[0]['data']['GenFuelMixes']['GenFuelMix']

            timestamp = None
            ne = self.MODEL()
            ne.gas = 0
            ne.nuclear = 0
            ne.hydro = 0
            ne.coal = 0
            ne.other_renewable = 0
            ne.other_fossil = 0

            marginal_fuel = len(MARGINAL_FUELS) - 1

            for i in json:
                if timestamp is None:
                    timestamp = i['BeginDate']

                fuel = i['FuelCategory']
                gen = i['GenMw']

                if fuel == 'Natural Gas':
                    ne.gas += gen
                elif fuel == 'Nuclear':
                    ne.nuclear += gen
                elif fuel == 'Hydro':
                    ne.hydro += gen
                elif fuel == 'Coal':
                    ne.coal += gen
                # I don't really know how I should be placing some of these fuels
                elif fuel == 'Oil' or fuel == 'Landfill Gas' or fuel == 'Refuse':
                    ne.other_fossil += gen
                elif fuel == 'Wind' or fuel == 'Wood':
                    ne.other_renewable += gen
                else: # Unrecognized fuel
                    ne.other_fossil += gen

                if i['MarginalFlag'] == 'Y':
                    if fuel in MARGINAL_FUELS:
                        marginal_fuel = min(marginal_fuel, MARGINAL_FUELS.index(fuel))

            ne.marginal_fuel = marginal_fuel
            ne.date_extracted = pytz.utc.localize(datetime.datetime.now())
            ne.fraction_clean = self._fraction_clean(ne)
            ne.total_MW = self._total_MW(ne)
            
            if timestamp is None:
                ne.date = None # Is this okay? Don't know.
            else:
                ne.date = dateutil.parser.parse(timestamp)

            if self.MODEL.objects.filter(date=ne.date,
                                         forecast_code=FORECAST_CODES['actual']).count() > 0:
                n_points = 0
            else:
                ne.save()
                n_points = 1

            return {'ba': 'ISONE', 'latest_date': str(self.MODEL.objects.latest().date), 'update_rows': n_points}
            
        except requests.exceptions.RequestException as e: # failed to get data
            return {'ba': 'ISONE', 'error': 'RequestException: %s' % e}
        except KeyError as e: # malformed json format
            return {'ba': 'ISONE', 'error': 'KeyError: %s' % e}
        except IndexError: # malformed json format
            return {'ba': 'ISONE', 'error': 'IndexError: %s' % e}
        except ValueError: # failed to parse time
            return {'ba': 'ISONE', 'error': 'ValueError: %s' % e}


###