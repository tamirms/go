package main

import (
	"bytes"
	"encoding"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/xdr"
)

var (
	issuer, _ = xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256{})

	nativeAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeNative,
	}
	nativeAssetString, _ = assetAsString(nativeAsset)

	usdAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: [4]byte{'u', 's', 'd', 0},
			Issuer:    issuer,
		},
	}
	usdAssetString, _ = assetAsString(usdAsset)

	eurAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: [4]byte{'e', 'u', 'r', 0},
			Issuer:    issuer,
		},
	}
	eurAssetString, _ = assetAsString(eurAsset)

	chfAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: [4]byte{'c', 'h', 'f', 0},
			Issuer:    issuer,
		},
	}
	chfAssetString, _ = assetAsString(chfAsset)

	yenAsset = xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: [4]byte{'y', 'e', 'n', 0},
			Issuer:    issuer,
		},
	}
	yenAssetString, _ = assetAsString(yenAsset)

	fiftyCentsOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(1),
		Buying:   usdAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 1,
			D: 2,
		},
		Amount: xdr.Int64(500),
	}
	quarterOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(2),
		Buying:   usdAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 1,
			D: 4,
		},
		Amount: xdr.Int64(500),
	}
	dollarOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(3),
		Buying:   usdAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}

	eurOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(4),
		Buying:   eurAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	twoEurOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(5),
		Buying:   eurAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	threeEurOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(6),
		Buying:   eurAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 3,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
)

func assertBinaryMarshalerEquals(t *testing.T, a, b encoding.BinaryMarshaler) {
	serializedA, err := a.MarshalBinary()
	if err != nil {
		t.Fatalf("could not marshal %v", a)
	}
	serializedB, err := b.MarshalBinary()
	if err != nil {
		t.Fatalf("could not marshal %v", b)
	}

	if !bytes.Equal(serializedA, serializedB) {
		t.Fatalf("expected lists to be equal got %v %v", a, b)
	}
}

func assertOfferListEquals(t *testing.T, a, b []xdr.OfferEntry) {
	if len(a) != len(b) {
		t.Fatalf("expected lists to have same length but got %v %v", a, b)
	}

	for i := 0; i < len(a); i++ {
		assertBinaryMarshalerEquals(t, a[i], b[i])
	}
}

func assertGraphEquals(t *testing.T, a, b OrderBookGraph) {
	if len(a.edgesForSellingAsset) != len(b.edgesForSellingAsset) {
		t.Fatalf("expected edges to have same length but got %v %v", a, b)
	}
	if len(a.tradingPairForOffer) != len(b.tradingPairForOffer) {
		t.Fatalf("expected trading pairs to have same length but got %v %v", a, b)
	}

	for sellingAsset, edgeSet := range a.edgesForSellingAsset {
		otherEdgeSet := b.edgesForSellingAsset[sellingAsset]
		if len(edgeSet) != len(otherEdgeSet) {
			t.Fatalf(
				"expected edge set for %v to have same length but got %v %v",
				sellingAsset,
				edgeSet,
				otherEdgeSet,
			)
		}
		for buyingAsset, offers := range edgeSet {
			otherOffers := otherEdgeSet[buyingAsset]

			if len(offers) != len(otherOffers) {
				t.Fatalf(
					"expected offers for %v to have same length but got %v %v",
					buyingAsset,
					offers,
					otherOffers,
				)
			}

			assertOfferListEquals(t, offers, otherOffers)
		}
	}

	for offerID, pair := range a.tradingPairForOffer {
		otherPair := b.tradingPairForOffer[offerID]
		if pair.buyingAsset != otherPair.buyingAsset {
			t.Fatalf(
				"expected trading pair to match but got %v %v",
				pair,
				otherPair,
			)
		}
		if pair.sellingAsset != otherPair.sellingAsset {
			t.Fatalf(
				"expected trading pair to match but got %v %v",
				pair,
				otherPair,
			)
		}
	}
}

func assertPathEquals(t *testing.T, a, b []Path) {
	if len(a) != len(b) {
		t.Fatalf("expected paths to have same length but got %v %v", a, b)
	}

	for i := 0; i < len(a); i++ {
		if a[i].SourceAmount != b[i].SourceAmount {
			t.Fatalf("expected paths to be same got %v %v", a, b)
		}
		if a[i].SourceAssetString != b[i].SourceAssetString {
			t.Fatalf("expected paths to be same got %v %v", a, b)
		}

		assertBinaryMarshalerEquals(t, a[i].SourceAsset, b[i].SourceAsset)

		if len(a[i].InteriorNodes) != len(b[i].InteriorNodes) {
			t.Fatalf("expected paths to be same got %v %v", a, b)
		}

		for j := 0; j > len(a[i].InteriorNodes); j++ {
			assertBinaryMarshalerEquals(t, a[i].InteriorNodes[j], b[i].InteriorNodes[j])
		}
	}
}

func TestAddEdgeSet(t *testing.T) {
	set := edgeSet{}

	set.add(dollarOffer, usdAssetString)
	set.add(eurOffer, eurAssetString)
	set.add(twoEurOffer, eurAssetString)
	set.add(threeEurOffer, eurAssetString)
	set.add(quarterOffer, usdAssetString)
	set.add(fiftyCentsOffer, usdAssetString)

	if len(set) != 2 {
		t.Fatalf("expected set to have 2 entries but got %v", set)
	}

	assertOfferListEquals(t, set[usdAssetString], []xdr.OfferEntry{
		quarterOffer,
		fiftyCentsOffer,
		dollarOffer,
	})

	assertOfferListEquals(t, set[eurAssetString], []xdr.OfferEntry{
		eurOffer,
		twoEurOffer,
		threeEurOffer,
	})
}

func TestRemoveEdgeSet(t *testing.T) {
	set := edgeSet{}

	if contains := set.remove(dollarOffer.OfferId, usdAssetString); contains {
		t.Fatal("expected set to not contain asset")
	}

	set.add(dollarOffer, usdAssetString)
	set.add(eurOffer, eurAssetString)
	set.add(twoEurOffer, eurAssetString)
	set.add(threeEurOffer, eurAssetString)
	set.add(quarterOffer, usdAssetString)
	set.add(fiftyCentsOffer, usdAssetString)

	if contains := set.remove(dollarOffer.OfferId, usdAssetString); !contains {
		t.Fatal("expected set to contain dollar offer")
	}

	if contains := set.remove(dollarOffer.OfferId, usdAssetString); contains {
		t.Fatal("expected set to not contain dollar offer after it has been deleted")
	}

	if contains := set.remove(threeEurOffer.OfferId, eurAssetString); !contains {
		t.Fatal("expected set to contain three euro offer")
	}
	if contains := set.remove(eurOffer.OfferId, eurAssetString); !contains {
		t.Fatal("expected set to contain euro offer")
	}
	if contains := set.remove(twoEurOffer.OfferId, eurAssetString); !contains {
		t.Fatal("expected set to contain two euro offer")
	}

	if contains := set.remove(eurOffer.OfferId, eurAssetString); contains {
		t.Fatal("expected set to not contain euro offer after it has been deleted")
	}

	if len(set) != 1 {
		t.Fatalf("expected set to have 1 entry but got %v", set)
	}

	assertOfferListEquals(t, set[usdAssetString], []xdr.OfferEntry{
		quarterOffer,
		fiftyCentsOffer,
	})
}

func TestAddOfferOrderBook(t *testing.T) {
	graph := NewOrderBookGraph()

	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(threeEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(eurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(twoEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(quarterOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(fiftyCentsOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	eurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(9),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	otherEurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(10),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}

	usdEurOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(11),
		Buying:   usdAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 3,
		},
		Amount: xdr.Int64(500),
	}

	if err := graph.Add(eurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	expectedGraph := OrderBookGraph{
		edgesForSellingAsset: map[string]edgeSet{
			nativeAssetString: edgeSet{
				usdAssetString: []xdr.OfferEntry{
					quarterOffer,
					fiftyCentsOffer,
					dollarOffer,
				},
				eurAssetString: []xdr.OfferEntry{
					eurOffer,
					twoEurOffer,
					threeEurOffer,
				},
			},
			usdAssetString: edgeSet{
				eurAssetString: []xdr.OfferEntry{
					eurUsdOffer,
					otherEurUsdOffer,
				},
			},
			eurAssetString: edgeSet{
				usdAssetString: []xdr.OfferEntry{
					usdEurOffer,
				},
			},
		},
		tradingPairForOffer: map[xdr.Int64]tradingPair{
			quarterOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			fiftyCentsOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			dollarOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			eurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			twoEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			threeEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			eurUsdOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: usdAssetString,
			},
			otherEurUsdOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: usdAssetString,
			},
			usdEurOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: eurAssetString,
			},
		},
	}

	// adding the same orders multiple times should have no effect
	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(threeEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	assertGraphEquals(t, graph, expectedGraph)
}

func TestUpdateOfferOrderBook(t *testing.T) {
	graph := NewOrderBookGraph()

	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(threeEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(eurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(twoEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(quarterOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(fiftyCentsOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	eurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(9),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	otherEurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(10),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}

	usdEurOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(11),
		Buying:   usdAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 3,
		},
		Amount: xdr.Int64(500),
	}

	if err := graph.Add(eurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	usdEurOffer.Price.N = 4
	usdEurOffer.Price.D = 1

	otherEurUsdOffer.Price.N = 1
	otherEurUsdOffer.Price.D = 2

	dollarOffer.Amount = 12

	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	expectedGraph := OrderBookGraph{
		edgesForSellingAsset: map[string]edgeSet{
			nativeAssetString: edgeSet{
				usdAssetString: []xdr.OfferEntry{
					quarterOffer,
					fiftyCentsOffer,
					dollarOffer,
				},
				eurAssetString: []xdr.OfferEntry{
					eurOffer,
					twoEurOffer,
					threeEurOffer,
				},
			},
			usdAssetString: edgeSet{
				eurAssetString: []xdr.OfferEntry{
					otherEurUsdOffer,
					eurUsdOffer,
				},
			},
			eurAssetString: edgeSet{
				usdAssetString: []xdr.OfferEntry{
					usdEurOffer,
				},
			},
		},
		tradingPairForOffer: map[xdr.Int64]tradingPair{
			quarterOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			fiftyCentsOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			dollarOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			eurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			twoEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			threeEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			eurUsdOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: usdAssetString,
			},
			otherEurUsdOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: usdAssetString,
			},
			usdEurOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: eurAssetString,
			},
		},
	}

	assertGraphEquals(t, graph, expectedGraph)
}

func TestRemoveOfferOrderBook(t *testing.T) {
	graph := NewOrderBookGraph()

	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(threeEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(eurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(twoEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(quarterOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(fiftyCentsOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	eurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(9),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	otherEurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(10),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}

	usdEurOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(11),
		Buying:   usdAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 3,
		},
		Amount: xdr.Int64(500),
	}

	if err := graph.Add(eurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Remove(usdEurOffer.OfferId); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Remove(otherEurUsdOffer.OfferId); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Remove(dollarOffer.OfferId); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Remove(dollarOffer.OfferId); err != errOfferNotPresent {
		t.Fatalf("expected error %v but got %v", errOfferNotPresent, err)
	}

	if err := graph.Remove(123456); err != errOfferNotPresent {
		t.Fatalf("expected error %v but got %v", errOfferNotPresent, err)
	}

	expectedGraph := OrderBookGraph{
		edgesForSellingAsset: map[string]edgeSet{
			nativeAssetString: edgeSet{
				usdAssetString: []xdr.OfferEntry{
					quarterOffer,
					fiftyCentsOffer,
				},
				eurAssetString: []xdr.OfferEntry{
					eurOffer,
					twoEurOffer,
					threeEurOffer,
				},
			},
			usdAssetString: edgeSet{
				eurAssetString: []xdr.OfferEntry{
					eurUsdOffer,
				},
			},
		},
		tradingPairForOffer: map[xdr.Int64]tradingPair{
			quarterOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			fiftyCentsOffer.OfferId: tradingPair{
				buyingAsset:  usdAssetString,
				sellingAsset: nativeAssetString,
			},
			eurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			twoEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			threeEurOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: nativeAssetString,
			},
			eurUsdOffer.OfferId: tradingPair{
				buyingAsset:  eurAssetString,
				sellingAsset: usdAssetString,
			},
		},
	}

	assertGraphEquals(t, graph, expectedGraph)
}

func TestConsumeOffers(t *testing.T) {
	var key xdr.Uint256
	kp, err := keypair.Random()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	copy(key[:], kp.Address())
	ignoreOffersFrom, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, key)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	otherSellerTwoEurOffer := twoEurOffer
	otherSellerTwoEurOffer.SellerId = ignoreOffersFrom

	denominatorZeroOffer := twoEurOffer
	denominatorZeroOffer.Price.D = 0

	for _, testCase := range []struct {
		name               string
		offers             []xdr.OfferEntry
		ignoreOffersFrom   xdr.AccountId
		currentAssetAmount xdr.Int64
		result             xdr.Int64
		err                error
	}{
		{
			"offers must not be empty",
			[]xdr.OfferEntry{},
			issuer,
			100,
			0,
			errorEmptyOffers,
		},
		{
			"currentAssetAmount must be positive",
			[]xdr.OfferEntry{eurOffer},
			ignoreOffersFrom,
			0,
			0,
			errorAssetAmountIsZero,
		},
		{
			"ignore all offers",
			[]xdr.OfferEntry{eurOffer},
			issuer,
			1,
			-1,
			nil,
		},
		{
			"offer denominator cannot be zero",
			[]xdr.OfferEntry{denominatorZeroOffer},
			ignoreOffersFrom,
			10000,
			0,
			errorOfferPriceDeonminatorIsZero,
		},
		{
			"ignore some offers",
			[]xdr.OfferEntry{eurOffer, otherSellerTwoEurOffer},
			issuer,
			100,
			200,
			nil,
		},
		{
			"not enough offers to consume",
			[]xdr.OfferEntry{eurOffer, twoEurOffer},
			ignoreOffersFrom,
			1001,
			-1,
			nil,
		},
		{
			"consume all offers",
			[]xdr.OfferEntry{eurOffer, twoEurOffer, threeEurOffer},
			ignoreOffersFrom,
			1500,
			3000,
			nil,
		},
		{
			"consume offer partially",
			[]xdr.OfferEntry{eurOffer, twoEurOffer},
			ignoreOffersFrom,
			2,
			2,
			nil,
		},
		{
			"round up",
			[]xdr.OfferEntry{quarterOffer},
			ignoreOffersFrom,
			5,
			2,
			nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := consumeOffers(
				testCase.offers,
				testCase.ignoreOffersFrom,
				testCase.currentAssetAmount,
			)
			if err != testCase.err {
				t.Fatalf("expected error %v but got %v", testCase.err, err)
			}
			if err == nil {
				if result != testCase.result {
					t.Fatalf("expected %v but got %v", testCase.result, result)
				}
			}
		})
	}

}

func TestSortAndFilterPaths(t *testing.T) {
	allPaths := []Path{
		Path{
			SourceAmount:      3,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      4,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      1,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      2,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      2,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes: []xdr.Asset{
				nativeAsset,
			},
		},
		Path{
			SourceAmount:      10,
			SourceAsset:       nativeAsset,
			SourceAssetString: nativeAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
	}
	sortedAndFiltered := sortAndFilterPaths(
		allPaths,
		3,
	)
	expectedPaths := []Path{
		Path{
			SourceAmount:      10,
			SourceAsset:       nativeAsset,
			SourceAssetString: nativeAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      2,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      2,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes: []xdr.Asset{
				nativeAsset,
			},
		},
		Path{
			SourceAmount:      3,
			SourceAsset:       eurAsset,
			SourceAssetString: eurAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      1,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
	}

	assertPathEquals(t, sortedAndFiltered, expectedPaths)
}

func TestFindPaths(t *testing.T) {
	graph := NewOrderBookGraph()

	if err := graph.Add(dollarOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(threeEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(eurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(twoEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(quarterOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := graph.Add(fiftyCentsOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	eurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(9),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}
	otherEurUsdOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(10),
		Buying:   eurAsset,
		Selling:  usdAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Amount: xdr.Int64(500),
	}

	usdEurOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(11),
		Buying:   usdAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 3,
		},
		Amount: xdr.Int64(500),
	}

	chfEurOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(12),
		Buying:   chfAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 2,
		},
		Amount: xdr.Int64(500),
	}

	yenChfOffer := xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(13),
		Buying:   yenAsset,
		Selling:  chfAsset,
		Price: xdr.Price{
			N: 1,
			D: 2,
		},
		Amount: xdr.Int64(500),
	}

	if err := graph.Add(eurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(otherEurUsdOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(usdEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(chfEurOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err := graph.Add(yenChfOffer); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	var key xdr.Uint256
	kp, err := keypair.Random()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	copy(key[:], kp.Address())
	ignoreOffersFrom, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, key)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	paths, err := graph.FindPaths(
		3,
		nativeAsset,
		20,
		ignoreOffersFrom,
		[]xdr.Asset{
			yenAsset,
			usdAsset,
		},
		[]xdr.Int64{
			100000,
			60000,
		},
		5,
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	expectedPaths := []Path{
		Path{
			SourceAmount:      5,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      7,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes: []xdr.Asset{
				eurAsset,
			},
		},
		Path{
			SourceAmount:      5,
			SourceAsset:       yenAsset,
			SourceAssetString: yenAssetString,
			InteriorNodes: []xdr.Asset{
				eurAsset,
				chfAsset,
			},
		},
	}

	assertPathEquals(t, paths, expectedPaths)

	paths, err = graph.FindPaths(
		4,
		nativeAsset,
		20,
		ignoreOffersFrom,
		[]xdr.Asset{
			yenAsset,
			usdAsset,
		},
		[]xdr.Int64{
			100000,
			60000,
		},
		5,
	)

	expectedPaths = []Path{
		Path{
			SourceAmount:      5,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes:     []xdr.Asset{},
		},
		Path{
			SourceAmount:      7,
			SourceAsset:       usdAsset,
			SourceAssetString: usdAssetString,
			InteriorNodes: []xdr.Asset{
				eurAsset,
			},
		},
		Path{
			SourceAmount:      2,
			SourceAsset:       yenAsset,
			SourceAssetString: yenAssetString,
			InteriorNodes: []xdr.Asset{
				usdAsset,
				eurAsset,
				chfAsset,
			},
		},
		Path{
			SourceAmount:      5,
			SourceAsset:       yenAsset,
			SourceAssetString: yenAssetString,
			InteriorNodes: []xdr.Asset{
				eurAsset,
				chfAsset,
			},
		},
	}

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	assertPathEquals(t, paths, expectedPaths)
}
